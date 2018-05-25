package vkr.nosave

import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._

object NdviTest {
  val b4Path = "wasb:///etl-experiments/mosaic"
  val b5Path = "wasb:///etl-experiments/mosaic-b5"
  val layerPath = "wasb:///vkr/ndvi/layer"

  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("yarn")
        .setAppName("NdviTest")
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

    val b4Rdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(b4Path)

    val layoutScheme = FloatingLayoutScheme(256)

    val (_, b4Metadata) =
      TileLayerMetadata.fromRdd(b4Rdd, layoutScheme)

    val b4Tiled: RDD[(SpatialKey, Tile)] =
      b4Rdd
        .tileToLayout(b4Metadata.cellType, b4Metadata.layout, Bilinear)


    val b5Rdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(b5Path)

    val (_, b5Metadata) =
      TileLayerMetadata.fromRdd(b5Rdd, layoutScheme)

    val b5Tiled: RDD[(SpatialKey, Tile)] =
      b4Rdd
        .tileToLayout(b5Metadata.cellType, b5Metadata.layout, Bilinear)


    val ndviRdd: RDD[(SpatialKey, Tile)] = b4Tiled.join(b5Tiled).combineValues {
      case (tile1: Tile, tile2: Tile) =>
        tile1.combineDouble(tile2) { (r, nir) =>
          if (isData(r) && isData(nir)) {
            (nir - r) / (nir + r)
          } else {
            Double.NaN
          }
        }
    }

    ndviRdd.cache()
  }
}
