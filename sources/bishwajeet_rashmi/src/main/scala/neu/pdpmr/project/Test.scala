package neu.pdpmr.project

import neu.pdpmr.compute.SongColnames
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object Test {
  private val schemaType = (new StructType)
    .add(SongColnames.NORM_TITLE, StringType)
    .add(SongColnames.NORM_ARTISTNAME, StringType)
    .add(SongColnames.MEAN_PRICE, DoubleType)
    .add(SongColnames.NORM_SONG_HOTNESS, DoubleType)

    .add(SongColnames.ARTIST_HOTNESS, DoubleType)
    .add(SongColnames.ARTIST_FAMILIARITY, DoubleType)
    .add(SongColnames.LOUDNESS, DoubleType)

    .add(SongColnames.DOW_CONF, DoubleType)
    .add(SongColnames.BIN_PLAYS, IntegerType)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("RFEngine")
      .getOrCreate

    val rdd = spark.sparkContext.parallelize(Seq(("mean_price", "song_hotness"), (1.5, 6.3)))

    val line = Seq( Row("", "", 0.89,0.733372,0.4613183,0.63990252, -4.769, 6414.00,1))
    val dataset = spark.createDataFrame(spark.sparkContext.parallelize(line), schemaType)
    dataset.show()
  }
}
