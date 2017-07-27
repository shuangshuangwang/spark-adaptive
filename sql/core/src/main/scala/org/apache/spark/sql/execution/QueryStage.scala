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

import org.apache.spark.MapOutputStatistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildLeft, BuildRight, SortMergeJoinExec}
import org.apache.spark.sql.execution.statsEstimation.Statistics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ThreadUtils

/**
 * QueryStageInput is the leaf node of a QueryStage and is used to hide its child stage.
 */
case class QueryStageInput(
    childStage: QueryStage,
    var isLocalShuffle: Boolean = false,
    var specifiedPartitionStartIndices: Option[Array[Int]] = None)
  extends LeafExecNode {

  override def outputPartitioning: Partitioning = specifiedPartitionStartIndices.map {
    indices => UnknownPartitioning(indices.length)
  }.getOrElse(childStage.outputPartitioning)

  override def output: Seq[Attribute] = childStage.output

  override def outputOrdering: Seq[SortOrder] = childStage.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    val childRDD = childStage.execute().asInstanceOf[ShuffledRowRDD]
    if (isLocalShuffle) {
      new LocalShuffledRowRDD(childRDD.dependency)
    } else {
      new ShuffledRowRDD(childRDD.dependency, specifiedPartitionStartIndices)
    }
  }

  override def computeStats(): Statistics = {
    childStage.stats
  }
}

case class QueryStage(var child: SparkPlan) extends UnaryExecNode {

  private var cachedRDD: RDD[InternalRow] = null

  private var mapOutputStatistics: MapOutputStatistics = null

  private val executionId = sqlContext.sparkContext.getLocalProperty(
    SQLExecution.EXECUTION_ID_KEY)

  private lazy val queryStageInputs: Seq[QueryStageInput] = child.collect {
    case input: QueryStageInput => input
  }

  def isLastStage: Boolean = child match {
    case _: ShuffleExchange => false
    case _ => true
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    if (cachedRDD == null) {
      // 1. Execute childStages
      // Use a thread pool to avoid blocking on one child stage.
      val childStages = queryStageInputs.map(_.childStage)
      if (childStages.length > 0) {
        val mapStageThreadPool =
          ThreadUtils.newDaemonFixedThreadPool(childStages.length, "adaptive-submit-stage-pool")
        var submitStageTasks = mutable.ArrayBuffer[Future[_]]()
        childStages.foreach { childStage =>
          submitStageTasks += mapStageThreadPool.submit(new Runnable {
            override def run(): Unit = {
              SQLExecution.withExecutionId(sqlContext.sparkContext, executionId) {
                childStage.execute()
              }
            }
          })
        }
        submitStageTasks.foreach(_.get())
        mapStageThreadPool.shutdown()
      }

      // 2. Optimize join in this stage based on previous stages' statistics.
      OptimizeJoin(conf).apply(this)
      // If the Joins are changed, we need apply EnsureRequirements rule to add BroadcastExchange.
      child = EnsureRequirements(conf).apply(child)

      // 3. Determine reducer number
      val childMapOutputStatistics = childStages.map(_.mapOutputStatistics)
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

      // 4. Execute plan in this stage
      val afterCodegen = CollapseCodegenStages(sqlContext.conf).apply(child)
      afterCodegen match {
        case exchange: ShuffleExchange =>
          // submit map stage and wait
          cachedRDD = exchange.eagerExecute()
          mapOutputStatistics = exchange.mapOutputStatistics
        case operator => // last stage
          cachedRDD = operator.execute()
      }
    }
    cachedRDD
  }
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

        val newQueryStageChild = queryStage.child.transformDown {
          case s: SortMergeJoinExec if (s.fastEquals(smj)) => broadcastJoin
        }
        // Apply EnsureRequirement rule to check if any new Exchange will be added. If no
        // Exchange is added, we convert the sortMergeJoin to BroadcastHashJoin. Otherwise
        // we don't convert it because it causes additional Shuffle.
        val afterEnsureRequirements = EnsureRequirements(conf).apply(newQueryStageChild)
        val numExchanges = afterEnsureRequirements.collect {
          case e: ShuffleExchange => e
        }.length

        if ((queryStage.isLastStage && numExchanges == 0) ||
          (!queryStage.isLastStage && numExchanges <= 1)) {
          // Set QueryStageInput to return local shuffled RDD
          broadcastJoin.foreach {
            case input: QueryStageInput => input.isLocalShuffle = true
            case _ =>
          }
          // Update the plan in queryStage
          queryStage.child = newQueryStageChild
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

    val newPlan = plan.transformUp {
      case operator: ShuffleExchange => QueryStageInput(QueryStage(operator))
    }
    QueryStage(newPlan)
  }
}
