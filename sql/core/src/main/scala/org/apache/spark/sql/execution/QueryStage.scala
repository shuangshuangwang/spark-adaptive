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
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ThreadUtils

case class QueryStage(child: SparkPlan, childStages: Seq[QueryStage]) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  private var cachedRDD: RDD[InternalRow] = null

  private var mapOutputStatistics: MapOutputStatistics = null

  private val executionId = sqlContext.sparkContext.getLocalProperty(
    SQLExecution.EXECUTION_ID_KEY)

  override def doExecute(): RDD[InternalRow] = {
    if (cachedRDD == null) {
      // 1. execute childStages
      // Use a thread pool to avoid blocking on one child stage.
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

      // 2. optimize join in this stage

      // 3. execute plan in this stage
      child match {
        case exchange: ShuffleExchange =>
          // submit map stage and wait
          val submittedStage = exchange.eagerExecute()
          mapOutputStatistics = submittedStage.get()
        case _ => // last stage
      }
      cachedRDD = child.doExecute()
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

  def findChildStages(
    plan: SparkPlan,
    childStages: ArrayBuffer[QueryStage]): Unit = plan match {
    case stage: QueryStage => childStages += stage
    case operator: SparkPlan =>
      operator.children.foreach(c => findChildStages(c, childStages))
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveExecutionEnabled) {
      return plan
    }
    plan.transformUp {
      case operator: ShuffleExchange =>
        val childStages = new ArrayBuffer[QueryStage]()
        findChildStages(operator, childStages)
        QueryStage(operator, childStages)
    }
  }
}
