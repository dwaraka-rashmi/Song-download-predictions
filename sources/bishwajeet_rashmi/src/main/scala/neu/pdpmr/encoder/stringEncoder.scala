package neu.pdpmr.encoder

import org.apache.spark.{SparkConf, SparkContext}
import java.io._
import java.io.Serializable

/**
  * created by Rashmi Dwaraka (dwarakarashmi@ccs.neu.edu)
  */
class Songs(row: String) extends Serializable {

  val columns = row.split(";") //All the columns from the row passed in the constructor

  val artist_name: String = columns(1)
  val title: String = columns(0)

}


object stringEncoder {

  def writeToFile(output: Array[String], outputPath: String): Unit = {
    val file = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(file))
    output.foreach(o => bw.write(o))
    bw.close()
  }



  def main(args: Array[String]): Unit = {

    // create Spark context with Spark configuration
     val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[*]"))
//    val sc = new SparkContext(new SparkConf().setAppName("Spark Encoder"))
    sc.setLogLevel("ERROR")

    // read input file paths and output path
    val songsInfo = sc.textFile(args(0))
    val opPath = args(1)

    //Remove the header row from the file and get the data
    val songsInfoData = songsInfo.mapPartitionsWithIndex {
      case (0, iter) => iter.drop(1)
      case (_, iter) => iter
    }.
      map(row => new Songs(row)).persist

    val artist_name_vocab = songsInfoData.map(s => s.artist_name).distinct().
      zipWithUniqueId().persist
    writeToFile(artist_name_vocab.map(s => s"${s._2},${s._1}\n").collect(),opPath+"artist_name_vocab.csv")

    val title_vocab = songsInfoData.map(s => s.title).distinct().
      zipWithUniqueId().persist
    writeToFile(title_vocab.map(s => s"${s._2},${s._1}\n").collect(),opPath+"title_vocab.csv")

    val encodedColumns = songsInfoData.map(s => (s.artist_name,s.title)).
      join(artist_name_vocab).map{case(artist_name,(title,artistId)) => (title,(artist_name,artistId))}.
      join(title_vocab).map{case(title,((artist_name,artist_id),title_id)) =>
      s"$artist_name,$artist_id,$title,$title_id\n"}.collect
    writeToFile(encodedColumns,opPath+"encoded_columns.csv")

    println("Done!")

  }

}
