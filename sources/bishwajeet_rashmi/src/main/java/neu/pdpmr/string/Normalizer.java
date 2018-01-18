package neu.pdpmr.string;

/**
 * @author deyb
 */
public class Normalizer {
    public static String normalize(String str) {
        return str.toLowerCase()
                .replaceAll("\\p{Punct}", "")
                .replaceAll("\\p{Space}{2,}", " ")
                .trim();
    }
}
