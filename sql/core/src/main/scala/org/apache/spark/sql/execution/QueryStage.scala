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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.exchange.{ExchangeCoordinator, ShuffleExchange}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ThreadUtils

/**
 * QueryStageInput is the leaf node of a QueryStage and is used to hide its child stage.
 */
case class QueryStageInput(childStage: QueryStage) extends LeafExecNode {

  var specifiedPartitionStartIndices: Option[Array[Int]] = None

  override def output: Seq[Attribute] = childStage.output

  override def doExecute(): RDD[InternalRow] = {
    val childRDD = childStage.doExecute().asInstanceOf[ShuffledRowRDD]
    new ShuffledRowRDD(childRDD.dependency, specifiedPartitionStartIndices)
  }
}

case class QueryStage(child: SparkPlan, stageInputs: Seq[QueryStageInput]) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  private var cachedRDD: RDD[InternalRow] = null

  private var mapOutputStatistics: MapOutputStatistics = null

  private val executionId = sqlContext.sparkContext.getLocalProperty(
    SQLExecution.EXECUTION_ID_KEY)

  override def doExecute(): RDD[InternalRow] = {
    if (cachedRDD == null) {
      // 1. execute childStages
      // Use a thread pool to avoid blocking on one child stage.
      val childStages = stageInputs.map(_.childStage)
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
      val childMapOutputStatistics = childStages.map(_.mapOutputStatistics).toArray

      // 2. optimize join in this stage


      // 3. determine reducer number
      val minNumPostShufflePartitions =
        if (conf.minNumPostShufflePartitions > 0) Some(conf.minNumPostShufflePartitions) else None

      val exchangeCoordinator = new ExchangeCoordinator(
        conf.targetPostShuffleInputSize,
        minNumPostShufflePartitions)
      val partitionStartIndices =
        exchangeCoordinator.estimatePartitionStartIndices(childMapOutputStatistics)
      stageInputs.foreach(_.specifiedPartitionStartIndices = Some(partitionStartIndices))

      // 4. execute plan in this stage
      val afterCodegen = CollapseCodegenStages(sqlContext.conf).apply(child)
      afterCodegen match {
        case exchange: ShuffleExchange =>
          // submit map stage and wait
          cachedRDD = exchange.eagerExecute()
          mapOutputStatistics = exchange.mapOutputStatistics
        case _ => // last stage
          cachedRDD = child.execute()
      }
    }
    cachedRDD
  }
}

case class PlanQueryStage(conf: SQLConf) extends Rule[SparkPlan] {
  //  private def insertQueryStage(plan: SparkPlan): SparkPlan = plan match {
  //    case exchange: ShuffleExchange =>
  //      val newChildren = exchange.children.map(insertQueryStage)
  //      QueryStage(exchange.withNewChildren(newChildren))
  //    case other =>
  //      other.withNewChildren(other.children.map(insertQueryStage))
  //  }
  //
  //  def apply(plan: SparkPlan): SparkPlan =  {
  //    insertQueryStage(plan)
  //  }

  def findStageInputs(
    plan: SparkPlan,
    stageInputs: ArrayBuffer[QueryStageInput]): Unit = plan match {
    case stage: QueryStageInput => stageInputs += stage
    case operator: SparkPlan =>
      operator.children.foreach(c => findStageInputs(c, stageInputs))
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveExecutionEnabled) {
      return plan
    }

    plan.transformUp {
      case operator: ShuffleExchange =>
        val queryStageInputs = new ArrayBuffer[QueryStageInput]()
        findStageInputs(operator, queryStageInputs)
        QueryStageInput(QueryStage(operator, queryStageInputs))
    }

    val stageInputs = new ArrayBuffer[QueryStageInput]()
    findStageInputs(plan, stageInputs)
    QueryStage(plan, stageInputs)
  }
}
