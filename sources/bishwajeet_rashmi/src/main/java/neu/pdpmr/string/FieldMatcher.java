package neu.pdpmr.string;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author deyb
 * @modified rashmidwaraka
 */
public class FieldMatcher {

    static Set<String> readArtists(final String path) throws IOException {
        return ((List<String>) IOUtils.readLines(new BufferedReader(
                new FileReader(path))))
                .stream()
                .map(line -> Normalizer.normalize(line))
                .collect(Collectors.toSet());
    }

    static Set<String> readTitle(final String path) throws IOException {
        return ((List<String>) IOUtils.readLines(new BufferedReader(
                new FileReader(path))))
                .stream()
                .map(line -> Normalizer.normalize(line))
                .collect(Collectors.toSet());
    }

    static Set<String> intersect(Set<String> first, Set<String> second) {
        final Set<String> firstCopy = new HashSet<>(first);

        firstCopy.retainAll(second);

        return firstCopy;
    }

    static Set<String> diff(Set<String> first, Set<String> second) {
        final Set<String> firstCopy = new HashSet<>(first);

        firstCopy.removeAll(second);

        return firstCopy;
    }

    private static void artistStats() throws IOException {
        final Set<String> downloadArtists = readArtists("/home/dey/git/pdpmr-final/input-full/match-artists/downloads_artists");
        final Set<String> millionSongArtists = readArtists("/home/dey/git/pdpmr-final/input-full/match-artists/song_artists");

        int matchFirst = intersect(millionSongArtists, downloadArtists).size();
        System.out.println("DOWNLOAD ARTISTS=" + downloadArtists.size());

        System.out.println("MILLION ARTISTS=" + millionSongArtists.size());
        System.out.println("MATCHES=" + matchFirst);
        new TreeSet<>(new HashSet<>(Arrays.asList("a", "b")));


        IOUtils.writeLines(
                new ArrayList<>(new TreeSet<>(diff(millionSongArtists, downloadArtists))),
                System.lineSeparator(),
                new FileWriter("/home/dey/git/pdpmr-final/input-full/match-artists/diff_artists"));
    }

    private static void titleStats() throws IOException {
        final Set<String> downloadTitle = readArtists("/home/dey/git/pdpmr-final/input-full/match-title/download_title");
        final Set<String> millionSongTitle = readArtists("/home/dey/git/pdpmr-final/input-full/match-title/song_title");

        int matchFirst = intersect(millionSongTitle, downloadTitle).size();
        System.out.println("DOWNLOAD TITLE=" + downloadTitle.size());

        System.out.println("MILLION TITLE=" + millionSongTitle.size());
        System.out.println("MATCHES=" + matchFirst);
        new TreeSet<>(new HashSet<>(Arrays.asList("a", "b")));

        IOUtils.writeLines(
                new ArrayList<>(new TreeSet<>(diff(millionSongTitle, downloadTitle))),
                System.lineSeparator(),
                new FileWriter("/home/dey/git/pdpmr-final/input-full/match-title/diff_title"));
    }

    private static void normalizeDownloads(String in, String out) throws IOException {
        List<String> normalizedLines = new ArrayList<>();

        for (String line : (List<String>) IOUtils.readLines(new BufferedReader(
                new FileReader(in)))) {
            String splits[] = line.split(";");
            splits[0] = Normalizer.normalize(splits[0]);
            splits[1] = Normalizer.normalize(splits[1]);

            normalizedLines.add(String.join(";", splits));
        }

        IOUtils.writeLines(
                normalizedLines,
                System.lineSeparator(),
                new FileWriter(out));
    }

    private static void normalizeSongInfoSubset(String in, String out) throws IOException {
        List<String> normalizedLines = new ArrayList<>();

        for (String line : (List<String>) IOUtils.readLines(new BufferedReader(
                new FileReader(in)))) {
            String splits[] = line.split(";");
            try {
                splits[2] = Normalizer.normalize(splits[2]);
                if (splits.length > 4) {
                    splits[4] = Normalizer.normalize(splits[4]);
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                System.err.println(line);
            }

            normalizedLines.add(String.join(";", splits));
        }

        IOUtils.writeLines(
                normalizedLines,
                System.lineSeparator(),
                new FileWriter(out));
    }

    public static void main(String[] args) throws IOException {
        //artistStats();
        //titleStats();

        String songInPath = args[0];
        String songOutPath = args[1];
        String downloadInPath = args[2];
        String downloadOutPath = args[3];

        normalizeDownloads(downloadInPath, downloadOutPath);
        normalizeSongInfoSubset(songInPath, songOutPath);
    }
}
