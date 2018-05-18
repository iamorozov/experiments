package tutorial

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.tiling._
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._

import scala.io.StdIn

object ChessPartitionerTest {

  def main(args: Array[String]): Unit = {
    // Setup Spark to use Kryo serializer.
    val conf =
      new SparkConf()
        .setMaster("yarn")
        .setAppName("ChessPartitionerTest")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    try {
      run(sc)
      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040
      println("Hit enter to exit.")
      StdIn.readLine()
    } finally {
      sc.stop()
    }
  }

  def run(implicit sc: SparkContext): Unit = {

    val inputRdd: RDD[(ProjectedExtent, Tile)] =
    sc.hadoopGeoTiffRDD("wasb:///data/LC81070352015218LGN00_B4.TIF")

    val (_, rasterMetaData) =
    TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(256))

    val tiled: RDD[(SpatialKey, Tile)] =
    inputRdd
      .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
      .partitionBy(new ChessPartitioner(100))

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    val rdd = TileLayerRDD(tiled, rasterMetaData)

    val store: HadoopAttributeStore = HadoopAttributeStore("wasb:///data/chess-layer")
    val writer = HadoopLayerWriter("wasb:///data/chess-layer", store)

    val layerId = LayerId("myLayer", 0)

    val index: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod
      .createIndex(rdd.metadata.bounds.asInstanceOf[KeyBounds[geotrellis.spark.SpatialKey]])

    writer.write(layerId, rdd, index)
  }
}