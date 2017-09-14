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

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

/**
 * This is a specialized version of [[org.apache.spark.sql.execution.ShuffledRowRDD]]. This is used
 * in Spark SQL adaptive execution to solve data skew issues. This RDD includes rearranged
 * partitions from mappers.
 *
 * This RDD takes a [[ShuffleDependency]] (`dependency`), a partitionIndex
 * and an array of map Id start indices as input arguments
 * (`specifiedMapIdStartIndices`).
 *
 */
class AdaptiveShuffledRowRDD(
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow],
    partitionIndex: Int,
    specifiedMapIdStartIndices: Option[Array[Int]] = None)
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  private[this] val numPostShufflePartitions = dependency.rdd.partitions.length

  private[this] val mapIdStartIndices: Array[Int] = specifiedMapIdStartIndices match {
    case Some(indices) => indices
    case None => (0 until numPostShufflePartitions).toArray
  }

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](mapIdStartIndices.length) { i =>
      val startIndex = mapIdStartIndices(i)
      val endIndex =
        if (i < mapIdStartIndices.length - 1) {
          mapIdStartIndices(i + 1)
        } else {
          numPostShufflePartitions
        }
      new ShuffledRowRDDPartition(
        i, partitionIndex, partitionIndex + 1, Some(startIndex), Some(endIndex))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val shuffledRowPartition = split.asInstanceOf[ShuffledRowRDDPartition]
    assert(shuffledRowPartition.startMapId.isDefined)
    assert(shuffledRowPartition.endMapId.isDefined)
    val reader = SparkEnv.get.shuffleManager.getReader(
      dependency.shuffleHandle,
      partitionIndex,
      context,
      shuffledRowPartition.startMapId.get,
      shuffledRowPartition.endMapId.get)
    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}
