package neu.pdpmr.model

import java.io._
import java.net.URI

import org.apache.hadoop.fs.Path
import au.com.bytecode.opencsv.CSVReader
import org.apache.commons.lang3.math.NumberUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
  *
  * @author deyb
  */
object SongInfos {
  private val log: Logger = Logger.getLogger(SongInfos.getClass)

  val DEFAULT_DURATION = -1000d
  val DEFAULT_LOUDNESS = -(1000d * 1000d)
  val DEFAULT_KEY_CONFIDENCE = -1000d
  val DEFAULT_ARTIST_FAMILIARITY = -1000d
  val DEFAULT_ARTIST_HOTNESS = -1000d
  val DEFAULT_SONG_HOTNESS = -1000d
  val DEFAULT_KEY = -1

  /**
    *
    * @param sc the underlying spark context
    * @param inputFiles String which contains a comma separated values of
    * @return persisted RDD of songs_info.csv
    */
  def persistSongInfo(sc: SparkContext, inputFiles: String): RDD[SongInfo] = {
    log.info("Starting loading from " + inputFiles)

    val textLines = sc.textFile(inputFiles)
      //filter header and empty lines
      .filter(line => !line.isEmpty() && !line.startsWith("track_id"))
      .map(line => {
        try {
          val r = new CSVReader(new StringReader(line), ';', '\0', false).readNext()
          val s = SongInfo(trackId = r(0), duration = NumberUtils.toDouble(r(5), DEFAULT_DURATION).doubleValue(),
            loudness = NumberUtils.toDouble(r(6), DEFAULT_LOUDNESS).doubleValue(),
            tempo = NumberUtils.toDouble(r(7), -1).doubleValue(),
            key = NumberUtils.toInt(r(8), DEFAULT_KEY).intValue(), keyConfidence = NumberUtils.toDouble(r(9), DEFAULT_KEY_CONFIDENCE).doubleValue(),
            artistId = r(16).trim, artistName = r(17).trim,
            artistFamiliarity = NumberUtils.toDouble(r(19), DEFAULT_ARTIST_FAMILIARITY).doubleValue(),
            artistHotness = NumberUtils.toDouble(r(20), DEFAULT_ARTIST_HOTNESS).doubleValue(),
            release = r(22).trim, songId = r(23).trim,
            title = r(24).trim, songHotness = NumberUtils.toDouble(r(25), DEFAULT_SONG_HOTNESS))

          Some(s)
        } catch  {
          case e: Exception => None
        }

      })
      .filter(_.isDefined) //remove none
      .map(_.get) //get only valid records
      .persist()
    log.info("Completed loading from " + inputFiles)
    textLines
  }


  def isValidLoudness(s: SongInfo): Boolean = {
     s.loudness != 0 && s.loudness > DEFAULT_LOUDNESS
  }

  def isValidDuration(s: SongInfo): Boolean = {
    s.duration > 0
  }

  def isValidTempo(s: SongInfo): Boolean = {
    s.tempo > 0
  }

  def isValidFamiliarity(s: SongInfo): Boolean = {
    s.artistFamiliarity > 0
  }

  def isValidArtistHotness(s: SongInfo): Boolean = {
    s.artistHotness > 0
  }

  def isValidKey(s: SongInfo): Boolean = {
    s.key >= 0
  }

  def isValidTitle(s: SongInfo): Boolean = {
    !(s.title.isEmpty)
  }

  /**
    * Write to file given by the {@code path}. Written as Semicolon separated values.
    * {@code headerIt} followed by all the values in {@code it}
    * @param it
    * @param headerIt
    * @param path
    */
  def write(it: Iterable[Iterable[String]], headerIt: Iterable[String], path: Path, sc: SparkContext): Unit = {
    val fs = FileSystem.get(path.toUri, sc.hadoopConfiguration)
    val pw: PrintWriter = new PrintWriter(new BufferedOutputStream(fs.create(path)))
    pw.println(headerIt.mkString(";"))
    it.foreach(x => pw.println(x.mkString(";")))
    pw.close
  }

}
