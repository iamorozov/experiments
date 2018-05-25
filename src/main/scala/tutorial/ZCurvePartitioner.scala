package tutorial

import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.{KeyBounds, SpatialKey}
import org.apache.spark.Partitioner

class ZCurvePartitioner(partitions: Int, bounds: KeyBounds[SpatialKey]) extends Partitioner {
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val zIndex = ZCurveKeyIndexMethod.createIndex(bounds)
    val spatialKey = key.asInstanceOf[SpatialKey]

    zIndex.toIndex(spatialKey).toInt % partitions
  }
}

object ZCurvePartitioner {
  def apply(partitions: Int, bounds: KeyBounds[SpatialKey]): ZCurvePartitioner =
    new ZCurvePartitioner(partitions, bounds)
}
