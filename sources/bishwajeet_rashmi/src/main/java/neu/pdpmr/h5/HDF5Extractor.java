package neu.pdpmr.h5;

import ncsa.hdf.object.h5.H5File;
import org.apache.log4j.Logger;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.StringJoiner;

import static neu.pdpmr.h5.hdf5_getters.*;


/**
 * @author deyb, rashmidwaraka
 */
public class HDF5Extractor {
    private static Logger log = Logger.getLogger(HDF5Extractor.class);

    /**
     * @param path
     * @return track_id
     * audio_md5
     * end_of_fade_in
     * start_of_fade_out
     * analysis_sample_rate
     * duration
     * loudness
     * tempo
     * key
     * key_confidence
     * mode
     * mode_confidence
     * time_signature
     * time_signature_confidence
     * danceability
     * energy
     * artist_id
     * artist_name
     * artist_location
     * artist_familiarity
     * artist_hotttnesss
     * genre
     * release
     * song_id
     * title
     * song_hotttnesss
     * year
     * <p>
     * artist_latitude
     * artist_longitude
     * similar_artists_length
     * artist_terms[0,5)
     * artist_terms_weight[0,4)
     * artist_term_value
     * mean_pitch
     * min_pitch
     * max_pitch
     * mean_timbre
     * mean_loudness
     * mean_beats
     * min_beats
     * max_beats
     * mean_tatums
     */
    public static String extractAsCSV(String path) throws Exception {
        path = path.replaceAll("^[^\\s]+:", "");
        log.info("Processing file:" + path);
        final StringJoiner joiner = new StringJoiner(";");
        H5File h5 = hdf5_open_readonly(path);

        joiner.add(get_track_id(h5));
//        joiner.add(get_audio_md5(h5));

//        joiner.add(String.format("%.3f", get_end_of_fade_in(h5)));
//        joiner.add(String.format("%.3f", get_start_of_fade_out(h5)));

        //joiner.add(String.format("%.3f", get_analysis_sample_rate(h5)));
//        joiner.add(String.format("%.3f", get_loudness(h5)));
//        joiner.add(String.format("%.3f", get_duration(h5)));
//        joiner.add(String.format("%.3f", get_tempo(h5)));

//        joiner.add(get_key(h5) + "");
//        joiner.add(String.format("%.3f", get_key_confidence(h5)));

//        joiner.add(get_mode(h5) + "");
//        joiner.add(String.format("%.3f", get_mode_confidence(h5)));

//        joiner.add(get_time_signature(h5) + "");
//        joiner.add(String.format("%.3f", get_time_signature_confidence(h5)));

//        joiner.add(String.format("%.3f", get_danceability(h5)));
//        joiner.add(String.format("%.3f", get_energy(h5)));

        joiner.add(get_artist_id(h5));
//        joiner.add(get_artist_name(h5));
//        joiner.add(get_artist_location(h5));
//        joiner.add(String.format("%.3f", get_artist_familiarity(h5)));
//        joiner.add(String.format("%.3f", get_artist_hotttnesss(h5)));
//
//        joiner.add(""); //genre
//        joiner.add(get_release(h5));
        joiner.add(get_song_id(h5));
//        joiner.add(get_title(h5));
//        joiner.add(String.format("%.3f", get_song_hotttnesss(h5)));
//        joiner.add(get_year(h5) + "");
//
//        joiner.add(String.format("%.3f", get_artist_latitude(h5)));
//        joiner.add(String.format("%.3f", get_artist_longitude(h5)));
//
//        joiner.add(get_similar_artists(h5).length + "");
//        joiner.add(getTopArtistTerms(h5, 3));
//
//        joiner.add(getTermFreq(h5, 3));

        joiner.add(String.format(getTimbreString(h5)));
        joiner.add(String.format(getPitchString(h5)));


//        joiner.add(String.format("%.3f", getMeanPitch(h5)));
//        joiner.add(String.format("%.3f", getMinPitch(h5)));
//        joiner.add(String.format("%.3f", getMaxPitch(h5)));
//        joiner.add(String.format("%.3f", getMeanTimbre(h5)));
//        joiner.add(String.format("%.3f", getMeanLoudness(h5)));
//        joiner.add(String.format("%.3f", getMeanBeats(h5)));
//        joiner.add(String.format("%.3f", getMinBeats(h5)));
//        joiner.add(String.format("%.3f", getMaxBeats(h5)));
//        joiner.add(String.format("%.3f", getMeanTatums(h5)));


        /*

        System.out.println("artist familiarity: " + get_artist_familiarity(h5));
        System.out.println("artist hotttnesss: " + get_artist_hotttnesss(h5));
        System.out.println("artist id: " + get_artist_id(h5));
        System.out.println("artist mbid: " + get_artist_mbid(h5));
        System.out.println("artist playmeid: " + get_artist_playmeid(h5));
        System.out.println("artist 7digitalid: " + get_artist_7digitalid(h5));
        System.out.println("artist latitude: " + get_artist_latitude(h5));
        System.out.println("artist longitude: " + get_artist_longitude(h5));
        System.out.println("artist location: " + get_artist_location(h5));
        System.out.println("artist name: " + get_artist_name(h5));
        System.out.println("release: " + get_release(h5));
        System.out.println("release 7digitalid: " + get_release_7digitalid(h5));
        System.out.println("song hotttnesss: " + get_song_hotttnesss(h5));
        System.out.println("title: " + get_title(h5));
        System.out.println("track 7digitalid: " + get_track_7digitalid(h5));
        resS = get_similar_artists(h5);
        System.out.println("similar artists, length: " + resS.length + ", elem 2: " + resS[20]);
        resS = get_artist_terms(h5);
        System.out.println("artists terms, length: " + resS.length + ", elem 0: " + resS[0]);
        res = get_artist_terms_freq(h5);
        System.out.println("artists terms freq, length: " + res.length + ", elem 0: " + res[0]);
        res = get_artist_terms_weight(h5);
        System.out.println("artists terms weight, length: " + res.length + ", elem 0: " + res[0]);
        // analysis
        System.out.println("duration: " + get_duration(h5));
        System.out.println("energy " + get_energy(h5));
        System.out.println("end_of_fade_in: " + get_end_of_fade_in(h5));
        System.out.println("key: " + get_key(h5));
        System.out.println("key confidence: " + get_key_confidence(h5));
        System.out.println("loudness: " + get_loudness(h5));
        System.out.println("mode: " + get_mode(h5));
        System.out.println("mode confidence: " + get_mode_confidence(h5));
        System.out.println("start of fade out: " + get_start_of_fade_out(h5));
        System.out.println("tempo: " + get_tempo(h5));
        System.out.println("time signature: " + get_time_signature(h5));
        System.out.println("time signature confidence: " + get_time_signature_confidence(h5));
        res = get_segments_start(h5);
        System.out.println("segments start, length: " + res.length + ", elem 20: " + res[20]);
        res = get_segments_confidence(h5);
        System.out.println("segments confidence, length: " + res.length + ", elem 20: " + res[20]);
        res = get_segments_pitches(h5);
        System.out.println("segments pitches, length: " + res.length + ", elem 20: " + res[20]);
        res = get_segments_timbre(h5);
        System.out.println("segments timbre, length: " + res.length + ", elem 20: " + res[20]);
        res = get_segments_loudness_max(h5);
        System.out.println("segments loudness max, length: " + res.length + ", elem 20: " + res[20]);
        res = get_segments_loudness_max_time(h5);
        System.out.println("segments loudness max time, length: " + res.length + ", elem 20: " + res[20]);
        res = get_segments_loudness_start(h5);
        System.out.println("segments loudness start, length: " + res.length + ", elem 20: " + res[20]);
        res = get_sections_start(h5);
        System.out.println("sections start, length: " + res.length + ", elem 1: " + res[1]);
        res = get_sections_confidence(h5);
        System.out.println("sections confidence, length: " + res.length + ", elem 1: " + res[1]);
        res = get_beats_start(h5);
        System.out.println("beats start, length: " + res.length + ", elem 1: " + res[1]);
        res = get_beats_confidence(h5);
        System.out.println("beats confidence, length: " + res.length + ", elem 1: " + res[1]);
        res = get_bars_start(h5);
        System.out.println("bars start, length: " + res.length + ", elem 1: " + res[1]);
        res = get_bars_confidence(h5);
        System.out.println("bars confidence, length: " + res.length + ", elem 1: " + res[1]);
        res = get_tatums_start(h5);
        System.out.println("tatums start, length: " + res.length + ", elem 3: " + res[3]);
        res = get_tatums_confidence(h5);
        System.out.println("tatums confidence, length: " + res.length + ", elem 3: " + res[3]);
        // musicbrainz
        System.out.println("year: " + get_year(h5));
        resS = get_artist_mbtags(h5);
        resI = get_artist_mbtags_count(h5);
        if (resS.length > 0) {
            System.out.println("artists mbtags, length: " + resS.length + ", elem 0: " + resS[0]);
            System.out.println("artists mbtags count, length: " + resI.length + ", elem 0: " + resI[0]);
        } else {
            System.out.println("artists mbtags, length: " + resS.length);
            System.out.println("artists mbtags count, length: " + resI.length);
        }
        */

        return joiner.toString();
    }

    private static int getArtistTagCount(H5File h5) throws Exception {
        int count[] = get_artist_mbtags_count(h5);
        return Arrays.stream(count).sum();
    }

    private static String getTatumsString(H5File h5) throws Exception {
        double tatums[] = get_beats_start(h5);
        StringJoiner tatumsStr = new StringJoiner(";");
        System.out.println("Tatums" + tatums.length);
        for (int i = 0; i < tatums.length; i++) {
            tatumsStr.add(String.format("%.3f", tatums[i]));
        }
        return tatumsStr.toString();
    }

    private static double getMeanTatums(H5File h5) throws Exception {
        double tatums[] = get_tatums_start(h5);

        return getAverage(tatums);
    }

    private static String getBeatsString(H5File h5) throws Exception {
        double beats[] = get_beats_start(h5);
        StringJoiner beatsStr = new StringJoiner(";");
        System.out.println("Beats" + beats.length);
        for (int i = 0; i < beats.length; i++) {
            beatsStr.add(String.format("%.3f", beats[i]));
        }
        return beatsStr.toString();
    }

    private static double getMinBeats(H5File h5) throws Exception {
        double beats[] = get_beats_start(h5);

        return Arrays.stream(beats).min().getAsDouble();
    }

    private static double getMaxBeats(H5File h5) throws Exception {
        double beats[] = get_beats_start(h5);

        return Arrays.stream(beats).max().getAsDouble();
    }

    private static String get12DVector(double[] measure) {
        int measureLength = measure.length;
        int bucketLength = Integer.valueOf(measureLength / 12);

        StringJoiner result = new StringJoiner(";");
        double sum = 0.0;
        for (int i = 0; i < measureLength; i++) {
            if ((i % bucketLength == 0 | i == measureLength - 1) & (i != 0)) {
                result.add(String.format("%.3f", sum / bucketLength));
                sum = 0.0;
            } else sum += measure[i];
        }

        return result.toString();
    }

    private static String getTimbreString(H5File h5) throws Exception {
        double timbre[] = get_segments_timbre(h5);
        return get12DVector(timbre);
    }

    private static String getPitchString(H5File h5) throws Exception {
        double pitch[] = get_segments_pitches(h5);
        return get12DVector(pitch);
    }

    private static double getMeanBeats(H5File h5) throws Exception {
        double beats[] = get_beats_start(h5);

        return getAverage(beats);
    }

    private static double getMeanLoudness(H5File h5) throws Exception {
        double loudness[] = get_segments_loudness_max(h5);

        return getAverage(loudness);
    }

    private static double getMeanTimbre(H5File h5) throws Exception {
        double timbre[] = get_segments_timbre(h5);

        return getAverage(timbre);
    }

    private static double getMeanPitch(H5File h5) throws Exception {
        double pitch[] = get_segments_pitches(h5);

        return getAverage(pitch);
    }

    private static double getMinPitch(H5File h5) throws Exception {
        double pitch[] = get_segments_pitches(h5);

        return Arrays.stream(pitch).min().getAsDouble();
    }

    private static double getMaxPitch(H5File h5) throws Exception {
        double pitch[] = get_segments_pitches(h5);

        return Arrays.stream(pitch).max().getAsDouble();
    }

    private static double getAverage(double num[]) throws Exception {
        if (num.length == 0) {
            return -1000.0;
        }

        return Arrays.stream(num).average().getAsDouble();
    }

    private static double getTermsValue(H5File h5, int max) throws Exception {
        double freq[] = get_artist_terms_freq(h5);
        double weights[] = get_artist_terms_weight(h5);
        double termValue = 0.0;

        for (int i = 0; i < Math.min(Math.min(freq.length, max), weights.length); i++) {
            termValue += freq[i] * weights[i];
        }

        return termValue;
    }

    private static String getTermFreq(H5File h5, int max) throws Exception {
        double freq[] = get_artist_terms_freq(h5);
        double weights[] = get_artist_terms_weight(h5);

        StringJoiner terms = new StringJoiner(";");
        int added = 0;
        for (int i = 0; i < Math.min(Math.min(freq.length, max), weights.length); i++, added++) {
            terms.add(String.format("%.3f", freq[i] * weights[i]));
        }

        for (int i = added; i < max; i++) {
            terms.add("-1000.000");
        }

        return terms.toString();
    }

    private static String getTopArtistTerms(H5File h5, int max) throws Exception {
        String[] artistTerms = get_artist_terms(h5);
        StringJoiner terms = new StringJoiner(";");
        int added = 0;
        for (int i = 0; i < Math.min(artistTerms.length, max); i++, added++) {
            terms.add(artistTerms[i]);
        }

        for (int i = added; i < max; i++) {
            terms.add("");
        }

        return terms.toString();
    }

    public static void main(String[] args) throws Exception {

        String[] extensions = new String[]{"h5"};
        Collection files = FileUtils.listFiles(new File(args[0]), extensions, true);
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(args[1])));

//        writer.write("track_id;artist_id;song_id;artist_term_value;mean_pitch;min_pitch;max_pitch;" +
//                "mean_timbre;mean_loudness;mean_beats;min_beats;max_beats;mean_tatums\n");
//        writer.flush();

        writer.write("track_id;artist_id;song_id;timbre1;timbre2;timbre3;timbre4;timbre5;timbre6;" +
                "timbre7;timbre8;timbre9;timbre10;timbre11;timbre12;pitch1;pitch2;pitch3;pitch4;pitch5;pitch6;" +
                "pitch7;pitch8;pitch9;pitch10;pitch11;pitch12\n");
        writer.flush();

        int filecount = files.size();
        System.out.println("Number of files to process: " + filecount + "\n");

        files.forEach(f -> {
            try {
                writer.write(extractAsCSV(f.toString()) + "\n");
                writer.flush();
            } catch (Throwable e) {
                System.out.println("Exception in processing " + f.toString() + "\n" + e.toString());
            }
        });

        writer.close();
    }
}
