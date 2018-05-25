package vkr.save

import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.tiling._
import geotrellis.spark.{TileLayerRDD, _}
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._

object ReprojectionTest {

  val inputPath = "wasb:///etl-experiments/mosaic"

  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("yarn")
        .setAppName("PyramidingTest")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    try {
      run(sc, args(0))
    } finally {
      sc.stop()
    }
  }

  def run(implicit sc: SparkContext, layerPath: String): Unit = {

    val inputRdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(inputPath)

    val layoutScheme = FloatingLayoutScheme(256)

    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, layoutScheme)

    val tiled: RDD[(SpatialKey, Tile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)

    val (zoom, rdd): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      TileLayerRDD(tiled, rasterMetaData)
      .reproject(WebMercator, layoutScheme, Bilinear)

    val store: HadoopAttributeStore = HadoopAttributeStore(layerPath)
    val writer = HadoopLayerWriter(layerPath, store)

    val layerId = LayerId("reprojection", zoom)

    val index: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod
      .createIndex(rdd.metadata.bounds.asInstanceOf[KeyBounds[geotrellis.spark.SpatialKey]])

    writer.write(layerId, rdd, index)
  }
}
