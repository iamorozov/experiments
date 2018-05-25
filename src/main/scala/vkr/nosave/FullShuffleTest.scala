package vkr.nosave

import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._
import tutorial.{ZCurvePartitioner, ZCurveReversePartitioner}

object FullShuffleTest {

  val inputPath = "wasb:///etl-experiments/mosaic"

  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("yarn")
        .setAppName("FullShuffleTest")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    try {
      run(sc)
    } finally {
      sc.stop()
    }
  }

  def run(implicit sc: SparkContext): Unit = {

    val inputRdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(inputPath)

    val layoutScheme = FloatingLayoutScheme(256)

    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, layoutScheme)

    val bounds = rasterMetaData.bounds.asInstanceOf[KeyBounds[SpatialKey]]

    val tiled: RDD[(SpatialKey, Tile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .partitionBy(ZCurvePartitioner(100, bounds))
        .cache()

    tiled
      .partitionBy(ZCurveReversePartitioner(100, bounds))
      .count()
  }
}
