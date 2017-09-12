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
    specifiedMapIdStartIndices: Array[Int])
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  private[this] val numPreShufflePartitions = dependency.partitioner.numPartitions

  private[this] val mapIdStartIndices: Array[Int] = specifiedMapIdStartIndices

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](mapIdStartIndices.length) { i =>
      val startIndex = mapIdStartIndices(i)
      val endIndex =
        if (i < mapIdStartIndices.length - 1) {
          mapIdStartIndices(i + 1)
        } else {
          numPreShufflePartitions
        }
      new ShuffledRowRDDPartition(
        i, partitionIndex, partitionIndex + 1, Some(startIndex), Some(endIndex))
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    tracker.getPreferredLocationsForShuffle(dep, partition.index)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val shuffledRowPartition = split.asInstanceOf[ShuffledRowRDDPartition]
    assert(shuffledRowPartition.startMapId.isDefined)
    assert(shuffledRowPartition.endMapId.isDefined)
    // The range of pre-shuffle partitions that we are fetching at here is
    // [0, numPreShufflePartitions - 1].
    val reader =
    SparkEnv.get.shuffleManager.getReader(
      dependency.shuffleHandle,
      partitionIndex,
      partitionIndex + 1,
      context,
      shuffledRowPartition.startMapId.getOrElse(0),
      shuffledRowPartition.endMapId.getOrElse(0))
    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}
