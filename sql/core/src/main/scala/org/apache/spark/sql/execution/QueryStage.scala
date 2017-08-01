/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.util.concurrent.Future

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.MapOutputStatistics
import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildLeft, BuildRight, SortMergeJoinExec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils

/**
 * QueryStageInput is the leaf node of a QueryStage and is used to hide its child stage.
 */
abstract class QueryStageInput extends LeafExecNode {

  def childStage: QueryStage

  override def output: Seq[Attribute] = childStage.output

  override def outputPartitioning: Partitioning = childStage.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = childStage.outputOrdering

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false): StringBuilder = {
    childStage.generateTreeString(depth, lastChildren, builder, verbose, "*")
  }

  override def computeStats: Statistics = {
    childStage.stats
  }
}

case class ShuffleQueryStageInput(
    childStage: QueryStage,
    var isLocalShuffle: Boolean = false,
    var specifiedPartitionStartIndices: Option[Array[Int]] = None)
  extends QueryStageInput {

  override def outputPartitioning: Partitioning = specifiedPartitionStartIndices.map {
    indices => UnknownPartitioning(indices.length)
  }.getOrElse(childStage.outputPartitioning)

  override def doExecute(): RDD[InternalRow] = {
    val childRDD = childStage.execute().asInstanceOf[ShuffledRowRDD]
    if (isLocalShuffle) {
      new LocalShuffledRowRDD(childRDD.dependency)
    } else {
      new ShuffledRowRDD(childRDD.dependency, specifiedPartitionStartIndices)
    }
  }
}

case class BroadcastQueryStageInput(childStage: QueryStage)
  extends QueryStageInput {

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    childStage.executeBroadcast()
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastStageInput does not support the execute() code path.")
  }
}

abstract class QueryStage extends UnaryExecNode {

  var child: SparkPlan

  protected var _mapOutputStatistics: MapOutputStatistics = null

  def mapOutputStatistics: MapOutputStatistics = _mapOutputStatistics

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  def executeChildStages(): Unit = {
    // Execute childStages. Use a thread pool to avoid blocking on one child stage.
    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)

    // Submit shuffle stages
    val shuffleQueryStages: Seq[ShuffleQueryStage] = child.collect {
      case ShuffleQueryStageInput(queryStage: ShuffleQueryStage, _, _) => queryStage
    }
    if (shuffleQueryStages.length > 0) {
      val mapStageThreadPool = ThreadUtils.newDaemonCachedThreadPool("adaptive-submit-stage-pool")
      var submitStageTasks = mutable.ArrayBuffer[Future[_]]()
      shuffleQueryStages.foreach { queryStage =>
        submitStageTasks += mapStageThreadPool.submit(
          new Runnable {
            override def run(): Unit = {
              SQLExecution.withExecutionId(sqlContext.sparkContext, executionId) {
                queryStage.execute()
              }
            }
          })
      }
      submitStageTasks.foreach(_.get())
      mapStageThreadPool.shutdown()
    }

    // Handle broadcast stages
    val broadcastQueryStages: Seq[BroadcastQueryStage] = child.collect {
      case BroadcastQueryStageInput(queryStage: BroadcastQueryStage) => queryStage
    }
    broadcastQueryStages.foreach(_.prepareBroadcast)

    // Handle reused stages
    val reusedQueryStages: Seq[ReusedQueryStage] = child.collect {
      case ShuffleQueryStageInput(reusedQueryStage: ReusedQueryStage, _, _) => reusedQueryStage
      case BroadcastQueryStageInput(reusedQueryStage: ReusedQueryStage) => reusedQueryStage
    }
    reusedQueryStages.foreach { reusedQueryStage =>
      val newExchange = reusedQueryStage.queryStage.child.asInstanceOf[Exchange]
        reusedQueryStage.child = ReusedExchangeExec(
          reusedQueryStage.child.output,
          newExchange)
    }

    // Optimize join in this stage based on previous stages' statistics.
    OptimizeJoin(conf).apply(this)
    // If the Joins are changed, we need apply EnsureRequirements rule to add BroadcastExchange.
    child = EnsureRequirements(conf).apply(child)
  }

  def executeStage(): RDD[InternalRow] = child.execute()

  private var cachedRDD: RDD[InternalRow] = null

  override def doExecute(): RDD[InternalRow] = {
    if (cachedRDD == null) {
      // 1. Execute childStages and optimize the plan in this stage
      executeChildStages()

      // 2. Determine reducer number
      val queryStageInputs: Seq[ShuffleQueryStageInput] = child.collect {
        case input: ShuffleQueryStageInput => input
      }
      val childMapOutputStatistics = queryStageInputs.map(_.childStage.mapOutputStatistics)
        .filter(_ != null).toArray
      if (childMapOutputStatistics.length > 0) {
        val minNumPostShufflePartitions =
          if (conf.minNumPostShufflePartitions > 0) Some(conf.minNumPostShufflePartitions) else None

        val exchangeCoordinator = new ExchangeCoordinator(
          conf.targetPostShuffleInputSize,
          minNumPostShufflePartitions)
        val partitionStartIndices =
          exchangeCoordinator.estimatePartitionStartIndices(childMapOutputStatistics)
        queryStageInputs.foreach(_.specifiedPartitionStartIndices = Some(partitionStartIndices))
      }

      // 3. Codegen and update the UI
      child = CollapseCodegenStages(sqlContext.conf).apply(child)
      val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      if (executionId != null && executionId.nonEmpty) {
        val queryExecution = SQLExecution.getQueryExecution(executionId.toLong)
        sparkContext.listenerBus.post(SparkListenerSQLAdaptiveExecutionUpdate(
          executionId.toLong,
          queryExecution.toString,
          SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan)))
      }

      // 4. Execute the plan in this stage
      cachedRDD = executeStage()
    }
    cachedRDD
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, "*")
  }

  override def computeStats: Statistics = {
    if (mapOutputStatistics != null) {
      val sizeInBytes = mapOutputStatistics.bytesByPartitionId.sum
      Statistics(sizeInBytes = sizeInBytes)
    } else {
      child.stats
    }
  }
}

case class ResultQueryStage(var child: SparkPlan) extends QueryStage

case class ShuffleQueryStage(var child: SparkPlan) extends QueryStage {
  override def executeStage(): RDD[InternalRow] = {
    child match {
      case e: ShuffleExchange =>
        val result = e.eagerExecute()
        _mapOutputStatistics = e.mapOutputStatistics
        result
      case _ => throw new IllegalArgumentException(
        "The child of ShuffleQueryStage must be a ShuffleExchange.")
    }
  }
}

case class BroadcastQueryStage(var child: SparkPlan) extends QueryStage {
  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  def prepareBroadcast() : Unit = {
    executeChildStages()
    child = CollapseCodegenStages(sqlContext.conf).apply(child)
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }
}

case class ReusedQueryStage(
    var child: SparkPlan,
    queryStage: QueryStage) extends QueryStage {

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def mapOutputStatistics: MapOutputStatistics = queryStage.mapOutputStatistics

  override def computeStats: Statistics = queryStage.stats
}

case class OptimizeJoin(conf: SQLConf) extends Rule[SparkPlan] {

  private def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti => true
    case j: ExistenceJoin => true
    case _ => false
  }

  private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter => true
    case _ => false
  }

  private def canBroadcast(plan: SparkPlan): Boolean = {
    plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.adaptiveBroadcastJoinThreshold
  }

  private def removeSort(plan: SparkPlan): SparkPlan = {
    plan match {
      case s: SortExec => s.child
      case p: SparkPlan => p
    }
  }

  private def optimizeSortMergeJoin(
      smj: SortMergeJoinExec,
      queryStage: QueryStage): SparkPlan = {
    smj match {
      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right) =>
        val broadcastSide = if (canBuildRight(joinType) && canBroadcast(right)) {
          Some(BuildRight)
        } else if (canBuildLeft(joinType) && canBroadcast(left)) {
          Some(BuildLeft)
        } else {
          None
        }
      broadcastSide.map { buildSide =>
        val broadcastJoin = BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, buildSide, condition, removeSort(left), removeSort(right))

        val newChild = queryStage.child.transformDown {
          case s: SortMergeJoinExec if (s.fastEquals(smj)) => broadcastJoin
        }
        // Apply EnsureRequirement rule to check if any new Exchange will be added. If no
        // Exchange is added, we convert the sortMergeJoin to BroadcastHashJoin. Otherwise
        // we don't convert it because it causes additional Shuffle.
        val afterEnsureRequirements = EnsureRequirements(conf).apply(newChild)
        val numExchanges = afterEnsureRequirements.collect {
          case e: ShuffleExchange => e
        }.length

        if ((numExchanges == 0) ||
          (queryStage.isInstanceOf[ShuffleQueryStage] && numExchanges <= 1)) {
          // Set QueryStageInput to return local shuffled RDD
          broadcastJoin.children.foreach {
            case input: ShuffleQueryStageInput => input.isLocalShuffle = true
            case _ =>
          }
          // Update the plan in queryStage
          queryStage.child = newChild
          broadcastJoin
        } else {
          smj
        }
      }.getOrElse(smj)
    }
  }

  private def optimizeJoin(
      operator: SparkPlan,
      queryStage: QueryStage): SparkPlan = {
    operator match {
      case smj: SortMergeJoinExec =>
        val op = optimizeSortMergeJoin(smj, queryStage)
        val optimizedChildren = op.children.map(optimizeJoin(_, queryStage))
        op.withNewChildren(optimizedChildren)
      case op =>
        val optimizedChildren = op.children.map(optimizeJoin(_, queryStage))
        op.withNewChildren(optimizedChildren)
    }
  }

  def apply(plan: SparkPlan): SparkPlan = plan match {
    case queryStage: QueryStage =>
      val optimizedPlan = optimizeJoin(queryStage.child, queryStage)
      queryStage.child = optimizedPlan
      queryStage
    case _ => plan
  }
}

case class PlanQueryStage(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveExecutionEnabled) {
      return plan
    }

    // Build a hash map using schema of exchanges to avoid O(N*N) sameResult calls.
    val stages = mutable.HashMap[StructType, ArrayBuffer[QueryStage]]()

    val newPlan = plan.transformUp {
      case exchange: Exchange =>
        val sameSchema = stages.getOrElseUpdate(exchange.schema, ArrayBuffer[QueryStage]())
        val samePlan = sameSchema.find { s =>
          exchange.sameResult(s.child)
        }
        if (samePlan.isDefined) {
          // Keep the output of this exchange, the following plans require that to resolve
          // attributes.
          val reusedExchange =
            ReusedExchangeExec(exchange.output, samePlan.get.child.asInstanceOf[Exchange])
          exchange match {
            case e: ShuffleExchange =>
              ShuffleQueryStageInput(ReusedQueryStage(reusedExchange, samePlan.get))
            case e: BroadcastExchangeExec =>
              BroadcastQueryStageInput(ReusedQueryStage(reusedExchange, samePlan.get))
          }
        } else {
          val queryStageInput = exchange match {
            case e: ShuffleExchange => ShuffleQueryStageInput(ShuffleQueryStage(e))
            case e: BroadcastExchangeExec => BroadcastQueryStageInput(BroadcastQueryStage(e))
          }
          sameSchema += queryStageInput.childStage
          queryStageInput
        }
    }
    newPlan match {
      case c: ExecutedCommandExec => c
      case other => ResultQueryStage(other)
    }
  }
}

/**
 * Find out duplicated exchanges in the spark plan, then use the same exchange for all the
 * references.
 */
case class ReuseQueryStage(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.exchangeReuseEnabled) {
      return plan
    }
    // Build a hash map using schema of exchanges to avoid O(N*N) sameResult calls.
    val stages = mutable.HashMap[StructType, ArrayBuffer[QueryStage]]()
    plan match {
      case input: QueryStageInput =>
        val exchange = input.childStage.child.asInstanceOf[Exchange]
    }
    plan.transformUp {
      case qs: QueryStage if (qs.child.isInstanceOf[Exchange]) =>
        val exchange = qs.child
        // the exchanges that have same results usually also have same schemas (same column names).
        val sameSchema = stages.getOrElseUpdate(exchange.schema, ArrayBuffer[QueryStage]())
        val samePlan = sameSchema.find { s =>
          exchange.sameResult(s.child)
        }
        if (samePlan.isDefined) {
          // Keep the output of this exchange, the following plans require that to resolve
          // attributes.
          val reusedExchange =
            ReusedExchangeExec(exchange.output, samePlan.get.child.asInstanceOf[Exchange])
          ReusedQueryStage(reusedExchange, samePlan.get)
        } else {
          sameSchema += qs
          qs
        }
    }
  }
}
