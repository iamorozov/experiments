package vkr.save

import geotrellis.raster._
import geotrellis.raster.render.ColorMap
import geotrellis.raster.resample._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.{TileLayerRDD, _}
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._

object PngRenderingTest {

  val inputPath = "wasb:///etl-experiments/mosaic-small"
  val dataFolder = "wasb:///vkr/render/png"

  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("yarn")
        .setAppName("PngRenderingTest")
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

    val tiled: RDD[(SpatialKey, Tile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)

    val rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      TileLayerRDD(tiled, rasterMetaData)

    val raster = rdd.stitch()

    val colorMap = ColorMap.fromStringDouble("0:ffffe5ff;0.1:f7fcb9ff;0.2:d9f0a3ff;0.3:addd8eff;0.4:78c679ff;0.5:41ab5dff;0.6:238443ff;0.7:006837ff;1:004529ff").get
    val ndviPng = raster.tile.renderPng(colorMap)
    withPngHadoopWriteMethods(ndviPng).write(dataFolder + "ndvi.png")(sc)
  }
}
