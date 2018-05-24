package tutorial

import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.{KeyBounds, SpatialKey}
import org.apache.spark.Partitioner

class ZCurveReversePartitioner(partitions: Int, bounds: KeyBounds[SpatialKey]) extends Partitioner {
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val zIndex = ZCurveKeyIndexMethod.createIndex(bounds)
    val spatialKey = key.asInstanceOf[SpatialKey]

    (zIndex.toIndex(bounds.maxKey) - zIndex.toIndex(spatialKey)).toInt % partitions
  }
}

object ZCurveReversePartitioner {
  def apply(partitions: Int, bounds: KeyBounds[SpatialKey]): ZCurveReversePartitioner =
    new ZCurveReversePartitioner(partitions, bounds)
}
