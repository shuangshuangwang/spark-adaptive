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

import org.apache.spark.{broadcast, MapOutputStatistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.statsEstimation.{PartitionStatistics, Statistics}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils

/**
 * QueryStageInput is the leaf node of a QueryStage and is used to hide its child stage.
 */
abstract class QueryStageInput extends LeafExecNode {

  def childStage: QueryStage

  // Ignore this wrapper for canonicalizing.
  override lazy val canonicalized: SparkPlan = childStage.canonicalized

  // `QueryStageInput` can have distinct set of output attribute ids from its childStage, we need
  // to update the attribute ids in `outputPartitioning` and `outputOrdering`.
  private lazy val updateAttr: Expression => Expression = {
    val originalAttrToNewAttr = AttributeMap(childStage.output.zip(output))
    e => e.transform {
      case attr: Attribute => originalAttrToNewAttr.getOrElse(attr, attr)
    }
  }

  override def outputPartitioning: Partitioning = childStage.outputPartitioning match {
    case h: HashPartitioning => h.copy(expressions = h.expressions.map(updateAttr))
    case other => other
  }

  override def outputOrdering: Seq[SortOrder] = {
    childStage.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
  }

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
    override val output: Seq[Attribute],
    var isLocalShuffle: Boolean = false,
    var skewedPartitions: Option[mutable.HashSet[Int]] = None,
    var partitionStartIndices: Option[Array[Int]] = None,
    var partitionEndIndices: Option[Array[Int]] = None)
  extends QueryStageInput {

  override def outputPartitioning: Partitioning = partitionStartIndices.map {
    indices => UnknownPartitioning(indices.length)
  }.getOrElse(super.outputPartitioning)

  override def doExecute(): RDD[InternalRow] = {
    val childRDD = childStage.execute().asInstanceOf[ShuffledRowRDD]
    if (isLocalShuffle) {
      new LocalShuffledRowRDD(childRDD.dependency, partitionStartIndices, partitionEndIndices)
    } else {
      new ShuffledRowRDD(childRDD.dependency, partitionStartIndices, partitionEndIndices)
    }
  }

  def numMapper(): Int = {
    val childRDD = childStage.execute().asInstanceOf[ShuffledRowRDD]
    childRDD.dependency.rdd.partitions.length
  }
}

/**
 * A QueryStageInput that reads part of a single partition.The partition is divided into several
 * splits and it only reads one of the splits ranging from startMapId to endMapId (exclusive).
 */
case class SkewedShuffleQueryStageInput(
    childStage: QueryStage,
    override val output: Seq[Attribute],
    partitionId: Int,
    startMapId: Int,
    endMapId: Int)
  extends QueryStageInput {

  override def doExecute(): RDD[InternalRow] = {
    val childRDD = childStage.execute ().asInstanceOf[ShuffledRowRDD]
    new AdaptiveShuffledRowRDD(
      childRDD.dependency,
      partitionId,
      Some(Array(startMapId)),
      Some(Array(endMapId)))
  }
}

case class BroadcastQueryStageInput(
    childStage: QueryStage,
    override val output: Seq[Attribute])
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

  // Ignore this wrapper for canonicalizing.
  override lazy val canonicalized: SparkPlan = child.canonicalized

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  def executeChildStages(): Unit = {
    // Execute childStages. Use a thread pool to avoid blocking on one child stage.
    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)

    val queryStageSubmitTasks = mutable.ArrayBuffer[Future[_]]()

    // Handle broadcast stages
    val broadcastQueryStages: Seq[BroadcastQueryStage] = child.collect {
      case BroadcastQueryStageInput(queryStage: BroadcastQueryStage, _) => queryStage
    }
    broadcastQueryStages.foreach { queryStage =>
      queryStageSubmitTasks += QueryStage.queryStageThreadPool.submit(
        new Runnable {
          override def run(): Unit = {
            queryStage.prepareBroadcast()
          }
        })
    }

    // Submit shuffle stages
    val shuffleQueryStages: Seq[ShuffleQueryStage] = child.collect {
      case ShuffleQueryStageInput(queryStage: ShuffleQueryStage, _, _, _, _, _) => queryStage
    }
    shuffleQueryStages.foreach { queryStage =>
      queryStageSubmitTasks += QueryStage.queryStageThreadPool.submit(
        new Runnable {
          override def run(): Unit = {
            SQLExecution.withExecutionId(sqlContext.sparkContext, executionId) {
              queryStage.execute()
            }
          }
        })
    }

    queryStageSubmitTasks.foreach(_.get())
  }

  def executeStage(): RDD[InternalRow] = child.execute()

  private var cachedRDD: Option[RDD[InternalRow]] = None
  private var cachedArray: Option[Array[InternalRow]] = None

  def doPreExecutionOptimization(): Unit = {
    // 1. Execute childStages and optimize the plan in this stage
    executeChildStages()

    // Optimize join in this stage based on previous stages' statistics.
    val oldChild = child
    OptimizeJoin(conf).apply(this)
    HandleSkewedJoin(conf).apply(this)
    // If the Joins are changed, we need apply EnsureRequirements rule to add BroadcastExchange.
    if (!oldChild.fastEquals(child)) {
      child = EnsureRequirements(conf).apply(child)
    }

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

      if (queryStageInputs.length == 2 && queryStageInputs.forall(_.skewedPartitions.isDefined)) {
        // If a skewed join is detected and optimized, we will omit the skewed partitions when
        // estimate the partition start and end indices.
        val (partitionStartIndices, partitionEndIndices) =
          exchangeCoordinator.estimatePartitionStartEndIndices(
            childMapOutputStatistics, queryStageInputs(0).skewedPartitions.get)
        queryStageInputs.foreach { i =>
          i.partitionStartIndices = Some(partitionStartIndices)
          i.partitionEndIndices = Some(partitionEndIndices)
        }
      } else {
        val partitionStartIndices =
          exchangeCoordinator.estimatePartitionStartIndices(childMapOutputStatistics)
        queryStageInputs.foreach(_.partitionStartIndices = Some(partitionStartIndices))
      }
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
  }

  override def doExecute(): RDD[InternalRow] = synchronized {
    cachedRDD match {
      case None =>
        doPreExecutionOptimization()
        cachedRDD = Some(executeStage())
      case Some(cached) =>
    }
    cachedRDD.get
  }

  override def executeCollect(): Array[InternalRow] = synchronized {
    cachedArray match {
      case None =>
        doPreExecutionOptimization()
        cachedArray = Some(child.executeCollect())
      case Some(cached) =>
    }
    cachedArray.get
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
}

object QueryStage {
  lazy val queryStageThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("adaptive-query-stage-pool")
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

  private var prepared = false

  def prepareBroadcast() : Unit = synchronized {
    if (!prepared) {
      executeChildStages()
      child = CollapseCodegenStages(sqlContext.conf).apply(child)
      // After child stages are completed, prepare() triggers the broadcast.
      prepare()
      prepared = true
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
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

  private[execution] def calculatePartitionStartEndIndices(
      rowStatisticsByPartitionId: Array[Long]): (Array[Int], Array[Int]) = {
    val partitionStartIndicies = ArrayBuffer[Int]()
    val partitionEndIndicies = ArrayBuffer[Int]()
    var continuousZeroFlag = false
    var i = 0
    for (rows <- rowStatisticsByPartitionId) {
      if (rows != 0 && !continuousZeroFlag) {
        partitionStartIndicies += i
        continuousZeroFlag = true
      } else if (rows == 0 && continuousZeroFlag) {
        partitionEndIndicies += i
        continuousZeroFlag = false
      }
      i += 1
    }
    if (continuousZeroFlag) {
      partitionEndIndicies += i
    }
    if (partitionStartIndicies.length == 0) {
      (Array(0), Array(0))
    } else {
      (partitionStartIndicies.toArray, partitionEndIndicies.toArray)
    }
  }

  // After transforming to BroadcastJoin from SortMergeJoin, local shuffle read should be used and
  // there's opportunity to read less partitions based on previous shuffle write results.
  private def optimizeForLocalShuffleReadLessPartitions(
      broadcastSidePlan: SparkPlan,
      childrenPlans: Seq[SparkPlan]) = {
    // All shuffle read should be local instead of remote
    childrenPlans.foreach {
      case input: ShuffleQueryStageInput =>
        input.isLocalShuffle = true
      case _ =>
    }
    // If there's shuffle write on broadcast side, then find the partitions with 0 rows and ignore
    // reading them in local shuffle read.
    broadcastSidePlan match {
      case broadcast: ShuffleQueryStageInput =>
        val (startIndicies, endIndicies) = calculatePartitionStartEndIndices(broadcast.childStage
          .stats.partStatistics.get.rowsByPartitionId)
        childrenPlans.foreach {
          case input: ShuffleQueryStageInput =>
            input.partitionStartIndices = Some(startIndicies)
            input.partitionEndIndices = Some(endIndicies)
          case _ =>
        }
      case _ =>
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
          val broadcastSidePlan = buildSide match {
            case BuildLeft => (removeSort(left))
            case BuildRight => (removeSort(right))
          }

          // Local shuffle read less partitions based on broadcastSide's row statistics
          optimizeForLocalShuffleReadLessPartitions(broadcastSidePlan, broadcastJoin.children)

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

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveJoinEnabled) {
      plan
    } else {
      plan match {
        case queryStage: QueryStage =>
          val optimizedPlan = optimizeJoin(queryStage.child, queryStage)
          queryStage.child = optimizedPlan
          queryStage
        case _ => plan
      }
    }
  }
}

case class HandleSkewedJoin(conf: SQLConf) extends Rule[SparkPlan] {

  private def isSizeSkewed(size: Long, medianSize: Long): Boolean = {
    size > medianSize * conf.adaptiveSkewedFactor &&
      size > conf.adaptiveSkewedSizeThreshold
  }

  private def isRowCountSkewed(rowCount: Long, medianRowCount: Long): Boolean = {
    rowCount > medianRowCount * conf.adaptiveSkewedFactor &&
      rowCount > conf.adaptiveSkewedRowCountThreshold
  }

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * spark.sql.adaptive.skewedPartitionFactor and also larger than
   * spark.sql.adaptive.skewedPartitionSizeThreshold, or if its row count is larger than
   * the median row count * spark.sql.adaptive.skewedPartitionFactor and also larger than
   * spark.sql.adaptive.skewedPartitionSizeThreshold.
   */
  private def isSkewed(
      stats: PartitionStatistics,
      partitionId: Int,
      medianSize: Long,
      medianRowCount: Long): Boolean = {
    isSizeSkewed(stats.bytesByPartitionId(partitionId), medianSize) ||
      isRowCountSkewed(stats.rowsByPartitionId(partitionId), medianRowCount)
  }

  private def medianSizeAndRowCount(stats: PartitionStatistics): (Long, Long) = {
    val bytesLen = stats.bytesByPartitionId.length
    val rowCountsLen = stats.rowsByPartitionId.length
    val bytes = stats.bytesByPartitionId.sorted
    val rowCounts = stats.rowsByPartitionId.sorted
    val medSize = if (bytes(bytesLen / 2) > 0) bytes(bytesLen / 2) else 1
    val medRowCount = if (rowCounts(rowCountsLen / 2) > 0) rowCounts(rowCountsLen / 2) else 1
    (medSize, medRowCount)
  }

  /**
   * We split the partition into several splits. Each split reads the data from several map outputs
   * ranging from startMapId to endMapId(exclusive). This method calculates the split number and
   * the startMapId for all splits.
   */
  private def estimateMapIdStartIndices(
      queryStageInput: ShuffleQueryStageInput,
      partitionId: Int,
      medianSize: Long,
      medianRowCount: Long): Array[Int] = {
    queryStageInput.childStage.stats.partStatistics match {
      case Some(stats) =>
        val size = stats.bytesByPartitionId(partitionId)
        val rowCount = stats.rowsByPartitionId(partitionId)
        val factor = Math.max(size / medianSize, rowCount / medianRowCount)
        // We don't want to split too much. Set 5 and mapper number as the maximum.
        val numSplits = Math.min(5, Math.min(factor.toInt, queryStageInput.numMapper))
        val numMapperInSplit = queryStageInput.numMapper / numSplits
        (0 until numSplits).map(_ * numMapperInSplit).toArray
      case None => Array(0)
    }
  }

  private def isSupported(joinType: JoinType, left: QueryStageInput, right: QueryStageInput)
      : Boolean = {
    (joinType == Inner || joinType == Cross || joinType == LeftSemi) &&
      left.childStage.stats.partStatistics.isDefined &&
      right.childStage.stats.partStatistics.isDefined
  }

  private def handleSkewedJoin(
      operator: SparkPlan,
      queryStage: QueryStage): SparkPlan = operator.transformUp {
    case smj @ SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
      SortExec(_, _, left: ShuffleQueryStageInput, _),
      SortExec(_, _, right: ShuffleQueryStageInput, _)) if (isSupported(joinType, left, right)) =>

      val leftStats = left.childStage.stats.partStatistics.get
      val rightStats = right.childStage.stats.partStatistics.get
      val numPartitions = leftStats.bytesByPartitionId.length
      val (leftMedSize, leftMedRowCount) = medianSizeAndRowCount(leftStats)
      val (rightMedSize, rightMedRowCount) = medianSizeAndRowCount(rightStats)

      val skewedPartitions = mutable.HashSet[Int]()
      val subJoins = mutable.ArrayBuffer[SparkPlan](smj)
      for (partitionId <- 0 until numPartitions) {
        val isLeftSkew = isSkewed(leftStats, partitionId, leftMedSize, leftMedRowCount)
        val isRightSkew = isSkewed(rightStats, partitionId, rightMedSize, rightMedRowCount)
        if (isLeftSkew || isRightSkew) {
          skewedPartitions += partitionId
          val leftMapIdStartIndices = if (isLeftSkew) {
            estimateMapIdStartIndices(left, partitionId, leftMedSize, leftMedRowCount)
          } else {
            Array(0)
          }
          val rightMapIdStartIndices = if (!isRightSkew || joinType == LeftSemi) {
            // For left semi join, we don't split the right partition
            Array(0)
          } else {
            estimateMapIdStartIndices(right, partitionId, rightMedSize, rightMedRowCount)
          }

          for (i <- 0 until leftMapIdStartIndices.length;
               j <- 0 until rightMapIdStartIndices.length) {
            val leftEndMapId = if (i == leftMapIdStartIndices.length - 1) {
              left.numMapper
            } else {
              leftMapIdStartIndices(i + 1)
            }
            val rightEndMapId = if (j == rightMapIdStartIndices.length - 1) {
              right.numMapper
            } else {
              rightMapIdStartIndices(j + 1)
            }

            val leftInput =
              SkewedShuffleQueryStageInput(
                left.childStage, left.output, partitionId, leftMapIdStartIndices(i), leftEndMapId)
            val rightInput =
              SkewedShuffleQueryStageInput(
                right.childStage, right.output, partitionId,
                rightMapIdStartIndices(j), rightEndMapId)

            subJoins +=
              SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, leftInput, rightInput)
          }
        }
      }
      if (skewedPartitions.size > 0) {
        left.skewedPartitions = Some(skewedPartitions)
        right.skewedPartitions = Some(skewedPartitions)
        UnionExec(subJoins.toList)
      } else {
        smj
      }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveSkewedJoinEnabled) {
      plan
    } else {
      plan match {
        case queryStage: QueryStage =>
          val queryStageInputs: Seq[ShuffleQueryStageInput] = queryStage.collect {
            case input: ShuffleQueryStageInput => input
          }
          if (queryStageInputs.length == 2) {
            // Currently we only support handling skewed join for 2 table join.
            val optimizedPlan = handleSkewedJoin(queryStage.child, queryStage)
            queryStage.child = optimizedPlan
            queryStage
          } else {
            queryStage
          }
        case _ => plan
      }
    }
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
          exchange match {
            case e: ShuffleExchange =>
              ShuffleQueryStageInput(samePlan.get, exchange.output)
            case e: BroadcastExchangeExec =>
              BroadcastQueryStageInput(samePlan.get, exchange.output)
          }
        } else {
          val queryStageInput = exchange match {
            case e: ShuffleExchange =>
              ShuffleQueryStageInput(ShuffleQueryStage(e), e.output)
            case e: BroadcastExchangeExec =>
              BroadcastQueryStageInput(BroadcastQueryStage(e), e.output)
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
