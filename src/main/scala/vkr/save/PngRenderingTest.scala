package vkr.save

import geotrellis.raster._
import geotrellis.raster.mapalgebra.local.Add
import geotrellis.raster.render.ColorMap
import geotrellis.raster.resample._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.{TileLayerRDD, _}
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._

object PngRenderingTest {

  val inputRed = "wasb:///etl-experiments/lansat-scene/LC08_L1TP_154032_20180401_20180401_01_RT_B4.TIF"
  val inputGreen = "wasb:///etl-experiments/lansat-scene/LC08_L1TP_154032_20180401_20180401_01_RT_B3.TIF"
  val inputBlue = "wasb:///etl-experiments/lansat-scene/LC08_L1TP_154032_20180401_20180401_01_RT_B2.TIF"

  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("yarn")
        .setAppName("PngRenderingTest")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    try {
      run(sc, args(0))
    } finally {
      sc.stop()
    }
  }

  def run(implicit sc: SparkContext, dataFolder: String): Unit = {

    val layoutScheme = FloatingLayoutScheme(256)


    val redRdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(inputRed)

    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(redRdd, layoutScheme)

    val redTiled: RDD[(SpatialKey, Tile)] =
      redRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)

    val red: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      TileLayerRDD(redTiled, rasterMetaData)


    val greenRdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(inputGreen)

    val greenTiled: RDD[(SpatialKey, Tile)] =
      greenRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)

    val green: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      TileLayerRDD(greenTiled, rasterMetaData)


    val blueRdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(inputBlue)

    val blueTiled: RDD[(SpatialKey, Tile)] =
      blueRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)

    val blue: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      TileLayerRDD(blueTiled, rasterMetaData)


    val rgbRdd = red.join(green).combineValues {
      case (red: Tile, green: Tile) => red.localMultiply(65536) + green.localMultiply(256)
    }
      .join(blue).combineValues(Add(_, _))


    val colorMap = ColorMap.fromStringDouble("0:ffffe5ff;0.1:f7fcb9ff;0.2:d9f0a3ff;0.3:addd8eff;0.4:78c679ff;0.5:41ab5dff;0.6:238443ff;0.7:006837ff;1:004529ff").get
    val ndviPng = rgbRdd.stitch().renderPng(colorMap)

    withPngHadoopWriteMethods(ndviPng).write(dataFolder + "ndvi.png")(sc)
  }
}
