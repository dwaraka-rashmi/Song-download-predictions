package neu.pdpmr.model

/**
  * This class holds the below values from songs_info.csv
  * @param trackId           Echo Nest Track ID
  * @param duration          in seconds
  * @param tempo             estimated tempo in BPM
  * @param artistId          Echo Nest Artist Id
  * @param artistName        artist name
  * @param artistFamiliarity algorithmic estimation
  * @param artistHotness     algorithmic estimation between 0 and 1
  * @param release           album name from which the track was taken
  * @param songId            Echo Nest Track ID
  * @param title             Song title
  * @param songHotness       algorithmic estimation between 0 and 1
  * @author deyb
  */
case class SongInfo(trackId: String, duration: Double, loudness: Double, tempo: Double,
  key: Integer, keyConfidence: Double,
  artistId: String, artistName: String, artistFamiliarity: Double,
  artistHotness: Double, release: String, songId: String, title: String,
  songHotness: Double) {
}