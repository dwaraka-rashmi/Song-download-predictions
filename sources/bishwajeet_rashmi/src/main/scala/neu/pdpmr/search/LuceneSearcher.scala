package neu.pdpmr.search

import java.io.{File, InputStream}
import java.nio.file.Paths
import java.util.Scanner

import org.apache.lucene.analysis.CharArraySet
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.search._
import org.apache.lucene.store.{FSDirectory, RAMDirectory}
import org.apache.spark.sql.Row;

/**
  * @author deyb(dey.b@husky.neu.edu)
  */
class LuceneSearcher(is: InputStream) {
  val searcher: IndexSearcher = init()

  def init(): IndexSearcher = {
    val analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET)

    val indexDir = new RAMDirectory()

    val writerConfig = new IndexWriterConfig(analyzer)

    val writer = new IndexWriter(indexDir, writerConfig)

    val scanner = new Scanner(is)

    var headers: Array[String] = null
    var firstLine = true

    //println("Starting reading:")
    while (scanner.hasNextLine()) {
      val splits = scanner.nextLine().split(";")

      val len = splits.length

      if (!firstLine) { //skip header
        val d = new Document()
        d.add(new TextField("norm_title", splits(0), Field.Store.YES))
        d.add(new TextField("norm_artistname", splits(1), Field.Store.YES))

        d.add(new StringField("norm_title_string", splits(0), Field.Store.YES))
        d.add(new StringField("norm_artistname_string", splits(1), Field.Store.YES))

        for (i <- 2 until len) {
          d.add(new StringField(headers(i), splits(i), Field.Store.YES))
        }

        writer.addDocument(d)
      } else {
        headers = splits
      }

      firstLine = false
    }

    writer.flush()
    writer.commit()
    writer.close()

    scanner.close()


    //println("Completed reading: ")
    val reader = DirectoryReader.open(indexDir)

    new IndexSearcher(reader)
  }

  def artistExists(artist: String): Boolean = {
    val hits = searcher.search(new TermQuery(new Term("norm_artistname_string", artist)), 1)

    return hits.totalHits > 0
  }

  def titleExists(title: String): Boolean = {
    val hits = searcher.search(new TermQuery(new Term("norm_title_string", title)), 1)

    return hits.totalHits > 0
  }

  def fuzzyArtist(artist: String, title: String): String = {
    val booleanQuery = new BooleanQuery.Builder()

    val titleQuery = new TermQuery(new Term("norm_title_string", title))

    booleanQuery.add(titleQuery, BooleanClause.Occur.MUST)
    //booleanQuery.add(new FuzzyQuery(new Term("norm_title", title)), BooleanClause.Occur.MUST)
    for (name <- artist.split("\\s+")) {
      booleanQuery.add(new FuzzyQuery(new Term("norm_artistname", name)), BooleanClause.Occur.MUST)
    }

    //booleanQuery.setMinimumNumberShouldMatch(2)

    val hits = searcher.search(booleanQuery.build(), 1)
    if (hits.totalHits == 0) {
      return null
    }

    return searcher.doc(hits.scoreDocs(0).doc).get("norm_artistname")
  }


  def searchFuzzyArtistTitle(artist: String, title: String): (String, String) = {
    val booleanQuery = new BooleanQuery.Builder()

    for (name <- title.split("\\s+")) {
      booleanQuery.add(new FuzzyQuery(new Term("norm_title", name)), BooleanClause.Occur.MUST)
    }

    for (name <- artist.split("\\s+")) {
      booleanQuery.add(new FuzzyQuery(new Term("norm_artistname", name)), BooleanClause.Occur.MUST)
    }


    val hits = searcher.search(booleanQuery.build(), 1)
    if (hits.totalHits == 0) {
      return (null, null)
    }

    val doc = searcher.doc(hits.scoreDocs(0).doc)

    return (doc.get("norm_artistname"), doc.get("norm_title"))

  }

  def fuzzyTitle(artist: String, title: String): String = {
    val booleanQuery = new BooleanQuery.Builder()

    val artistQuery = new TermQuery(new Term("norm_artistname_string", artist))

    booleanQuery.add(artistQuery, BooleanClause.Occur.MUST)
    for (name <- title.split("\\s+")) {
      booleanQuery.add(new FuzzyQuery(new Term("norm_title", name)), BooleanClause.Occur.MUST)
    }

    val hits = searcher.search(booleanQuery.build(), 1)
    if (hits.totalHits == 0) {
      return null
    }

    return searcher.doc(hits.scoreDocs(0).doc).get("norm_title")
  }

  def searchExact(artist: String, title: String): Row = {
    val booleanQuery = new BooleanQuery.Builder()
    val artistQuery = new TermQuery(new Term("norm_artistname_string", artist))
    val titleQuery = new TermQuery(new Term("norm_title_string", title))

    booleanQuery.add(artistQuery, BooleanClause.Occur.MUST)
    booleanQuery.add(titleQuery, BooleanClause.Occur.MUST)

    val hits = searcher.search(booleanQuery.build(), 1)
    if (hits.totalHits == 0) {
      return null
    }

    val doc = searcher.doc(hits.scoreDocs(0).doc)

    /*hits.scoreDocs.foreach(d => {
      val docId = d.doc
      val doc = searcher.doc(docId)

      println(s"Mean-Price:${doc.get("mean_price")}; Norm-Song-Hotness:${doc.get("norm_song_hotness")}")
    })*/
    Row(doc.get("norm_title"), doc.get("norm_artistname"),
         doc.get("mean_price").toDouble, doc.get("norm_song_hotness").toDouble, doc.get("artist_hotttnesss").toDouble,
         doc.get("artist_familiarity").toDouble, doc.get("loudness").toDouble, doc.get("dow_conf").toDouble, doc.get("bin_plays").toInt)
  }
}
