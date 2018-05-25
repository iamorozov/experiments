package vkr.save

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.resample._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.tiling._
import geotrellis.spark.{TileLayerRDD, _}
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._

object SobelFilterTest {

  val inputPath = "wasb:///etl-experiments/mosaic"
  val layerPath = "wasb:///vkr/sobel/layer"

  val kernelX = Kernel(ArrayTile.fromBytes(Array(-1, 0, 1, -2, 0, 2, -1, 0, 1), CellType.fromName("int32"), 3, 3))
  val kernelY = Kernel(ArrayTile.fromBytes(Array(-1, -2, -1, 0, 0, 0, 1, 2, 1), CellType.fromName("int32"), 3, 3))

  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("yarn")
        .setAppName("SobelFilterTest")
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

    val convolutionX = rdd.focalConvolve(kernelX)
    val convolutionY = rdd.focalConvolve(kernelY)

    val sobelRdd: RDD[(SpatialKey, Tile)] = convolutionX.join(convolutionY).combineValues {
      case (tile1: Tile, tile2: Tile) =>
        tile1.combineDouble(tile2) { (x, y) =>
          if (isData(x) && isData(y)) {
            Math.sqrt((x * x) + (y * y))
          } else {
            Double.NaN
          }
        }
    }

    val sobelTileLayerRDD: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      TileLayerRDD(sobelRdd, rasterMetaData)

    val store: HadoopAttributeStore = HadoopAttributeStore(layerPath)
    val writer = HadoopLayerWriter(layerPath, store)

    val layerId = LayerId("sobel", 0)

    val index: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod
      .createIndex(sobelTileLayerRDD.metadata.bounds.asInstanceOf[KeyBounds[geotrellis.spark.SpatialKey]])

    writer.write(layerId, sobelTileLayerRDD, index)
  }
}
