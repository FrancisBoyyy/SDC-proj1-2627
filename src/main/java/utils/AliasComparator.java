package utils;

import java.util.Comparator;

/**
 * Orders aliases such as node1, node2, ..., node10 (or client1, ..., client10)
 * by their prefix and then by their numeric suffix, so that every process
 * derives exactly the same ordering for the membership (and therefore, for
 * instance, the same primary for a given view).
 */
public final class AliasComparator implements Comparator<String> {
    public static final AliasComparator INSTANCE = new AliasComparator();

    private AliasComparator() {
    }

    @Override
    public int compare(String a, String b) {
        int prefixA = prefixLength(a);
        int prefixB = prefixLength(b);
        int cmp = a.substring(0, prefixA).compareTo(b.substring(0, prefixB));
        if (cmp != 0) {
            return cmp;
        }
        String suffixA = a.substring(prefixA);
        String suffixB = b.substring(prefixB);
        if (!suffixA.isEmpty() && !suffixB.isEmpty() && suffixA.length() < 18 && suffixB.length() < 18) {
            cmp = Long.compare(Long.parseLong(suffixA), Long.parseLong(suffixB));
            if (cmp != 0) {
                return cmp;
            }
        }
        return a.compareTo(b);
    }

    private static int prefixLength(String s) {
        int i = s.length();
        while (i > 0 && Character.isDigit(s.charAt(i - 1))) {
            i--;
        }
        return i;
    }
}
