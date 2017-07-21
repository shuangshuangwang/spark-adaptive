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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf

class QueryStageSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var originalActiveSparkSession: Option[SparkSession] = _
  private var originalInstantiatedSparkSession: Option[SparkSession] = _

  override protected def beforeAll(): Unit = {
    originalActiveSparkSession = SparkSession.getActiveSession
    originalInstantiatedSparkSession = SparkSession.getDefaultSession

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override protected def afterAll(): Unit = {
    // Set these states back.
    originalActiveSparkSession.foreach(ctx => SparkSession.setActiveSession(ctx))
    originalInstantiatedSparkSession.foreach(ctx => SparkSession.setDefaultSession(ctx))
  }

  def withSparkSession(f: SparkSession => Unit): Unit = {
    val sparkConf =
      new SparkConf(false)
        .setMaster("local[*]")
        .setAppName("test")
        .set("spark.ui.enabled", "false")
        .set("spark.driver.allowMultipleContexts", "true")
        .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(SQLConf.ADAPTIVE_BROADCASTJOIN_THRESHOLD.key, "12000")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    try f(spark) finally spark.stop()
  }

  val numInputPartitions: Int = 10

  def checkAnswer(actual: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(actual, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  test("sort merge join to broadcast join") {
    withSparkSession { spark: SparkSession =>
      val df1 =
        spark
          .range(0, 1000, 1, numInputPartitions)
          .selectExpr("id % 500 as key1", "id as value1")
      val df2 =
        spark
          .range(0, 1000, 1, numInputPartitions)
          .selectExpr("id % 500 as key2", "id as value2")

      val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("value2"))

      // Before Execution, there is one SortMergeJoin
      val SmjBeforeExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(SmjBeforeExecution.length === 1)

      // Check the answer.
      val expectedAnswer =
        spark
          .range(0, 1000)
          .selectExpr("id % 500 as key", "id as value")
          .union(spark.range(0, 1000).selectExpr("id % 500 as key", "id as value"))
      checkAnswer(
        join,
        expectedAnswer.collect())

      // During execution, the SortMergeJoin is changed to BroadcastHashJoinExec
      val SmjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(SmjAfterExecution.length === 0)

      val numBhjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: BroadcastHashJoinExec => smj
      }.length
      assert(numBhjAfterExecution === 1)

      val queryStageInputs = join.queryExecution.executedPlan.collect {
        case q: QueryStageInput => q
      }
      assert(queryStageInputs.length === 2)
    }
  }
}
