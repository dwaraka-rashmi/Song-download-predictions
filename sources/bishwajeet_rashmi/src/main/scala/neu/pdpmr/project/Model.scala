package neu.pdpmr.project

import java.io._
import java.nio.file._
import java.util.{Collections, Scanner}
import java.util.zip.GZIPInputStream

import neu.pdpmr.compute.{RFEngine, SongColnames}
import neu.pdpmr.search.LuceneSearcher
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.math.NumberUtils
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}

/**
  * @author deyb
  */
object Model {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.OFF)

    val tempDirPath = scala.util.Properties.envOrElse("TEMP_DIR_PATH", "/tmp")
    //println(s"${tempDirPath}")
    val modelStream = getClass.getClassLoader().getResourceAsStream("model.tar.gz")
    Files.copy(modelStream, Paths.get(s"${tempDirPath}/model.tar.gz"), StandardCopyOption.REPLACE_EXISTING)

    val pb = new ProcessBuilder("tar", "xfz", s"${tempDirPath}/model.tar.gz", "-C", s"${tempDirPath}")
    val p = pb.start()

    val tarReturn = p.waitFor()

    //Files.deleteIfExists(Paths.get(s"${tempDirPath}/model.tar.gz"))
    //println(s"Untar returned with ${c}")

    val featureStream: InputStream = getDFStream(args)
    //println(s"FeatureStream:${featureStream}")
    val searcher = new LuceneSearcher(new GZIPInputStream(getDFStream(args),
                                                           4 * 1024 * 1024))
    val in = new Scanner(System.in)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Model")
      .getOrCreate
    spark.sparkContext.setLogLevel("OFF")

    val df = readMissingDF(new GZIPInputStream(getDFStream(args),
                                                4 * 1024 * 1024), spark)
    val model = RandomForestRegressionModel.load(s"${tempDirPath}/model")

    while (in.hasNextLine()) {
      val line = in.nextLine()
      //println(s"Reading: ${line}")
      val splits = line
        .split(";")
        .map(_.trim)
      val (artist, song) = (normalize(splits(0)), normalize(splits(1)))

      var row = searcher.searchExact(artist, song)
      if (row == null) { //no exact matches found
        if (searcher.artistExists(artist)) { //we at least know the artist
          //println("Getting by median. Not found")
          val fuzzyTitle = searcher.fuzzyTitle(artist, song)
          //println(s">Artist exists fuzzy title: ${fuzzyTitle}")

          if (fuzzyTitle != null) {
            row = searcher.searchExact(artist, fuzzyTitle)
          } else {
            row = getMedianByArtist(df, artist, song)
          }
        } else if (searcher.titleExists(song)) { //if title exists
          val fuzzyArtist = searcher.fuzzyArtist(artist, song)
          //println(s">Title exists fuzzy artist: ${fuzzyArtist}")

          if (fuzzyArtist != null) {
            row = searcher.searchExact(fuzzyArtist, song)
          }
        } else {
          val (fuzzyArtist, fuzzySong) = searcher.searchFuzzyArtistTitle(artist, song)

          //println(s">Choose fuzzy artist: ${fuzzyArtist}, fuzzy song: ${fuzzySong}")

          if (fuzzyArtist != null && fuzzySong != null) {
            row = searcher.searchExact(fuzzyArtist, fuzzySong)
          }
        }
      }

      var downloads = 0

      if (row != null) {
        //println(">>Row" + row)
        downloads = RFEngine.predict(model, spark, row)
      }

      println(downloads)
    }
  }

  private def getDFStream(args: Array[String]) = {
    val featureStream = if (args.length > 1) new FileInputStream(args(1)) else getClasspathStream("cleandf.csv.gz")
    featureStream
  }

  private def getClasspathStream(file: String) = {
    getClass().getClassLoader().getResourceAsStream(file)
  }

  def normalize(str: String): String =
    str.toLowerCase.replaceAll("\\p{Punct}", "").replaceAll("\\p{Space}{2,}", " ").trim

  def getMedianByArtist(df: DataFrame, artist: String, song: String): Row = {
    val medians = df.filter(df(SongColnames.NORM_ARTISTNAME) === artist)
      .stat
      .approxQuantile(Array(SongColnames.MEAN_PRICE, SongColnames.NORM_SONG_HOTNESS, SongColnames.ARTIST_HOTNESS,
                             SongColnames.ARTIST_FAMILIARITY, SongColnames.LOUDNESS, SongColnames.DOW_CONF,
                             SongColnames.BIN_PLAYS),
                       Array(0.5),
                       0.1)
    //medians(medians.length - 1) = medians(medians.length - 1).toInt

    Row(song, artist,
         medians(0)(0), medians(1)(0), medians(2)(0), medians(3)(0), medians(4)(0), medians(5)(0), medians(6)(0).toInt)
  }

  def readMissingDF(is: InputStream, spark: SparkSession): DataFrame = {
    val reader = new BufferedReader(new InputStreamReader(is))
    var done = false
    var seenHeader = false

    val lines = ListBuffer[Row]()

    while (!done) {
      val line = reader.readLine()

      if (line == null) {
        done = true
      } else if (seenHeader) {
        val splits = line.split(";")

        lines.append(Row(splits(0), //norm_title
                          splits(1), //norm_artistname
                          NumberUtils.toDouble(splits(2), 0.0), //mean_price
                          NumberUtils.toDouble(splits(3), 0.0), //norm_songhotness
                          NumberUtils.toDouble(splits(4), 0.0), //artist_hotttnesss
                          NumberUtils.toDouble(splits(5), 0.0), //artist_familiarity
                          NumberUtils.toDouble(splits(6), 0.0), //loudness
                          NumberUtils.toDouble(splits(7), 0.0), //dow_conf
                          NumberUtils.toInt(splits(8), 0) //bin_plays
                        ))

      }

      seenHeader = true
    }

    reader.close()

    spark.createDataFrame(spark.sparkContext.parallelize(lines.toSeq), RFEngine.schemaType)
  }
}
