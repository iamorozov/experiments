package tutorial

import geotrellis.spark.SpatialKey
import org.apache.spark.Partitioner

class ChessPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val spatialKey = key.asInstanceOf[SpatialKey]
    val (col, row): (Int, Int) = spatialKey

    (col + row) % 2
  }
}

object ChessPartitioner {
  def apply(partitions: Int): ChessPartitioner =
    new ChessPartitioner(partitions)
}
