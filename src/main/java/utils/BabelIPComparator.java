package utils;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import pt.unl.fct.di.novasys.babel2.channels.BabelNodeComponent;
import pt.unl.fct.di.novasys.babel2.channels.IPBabelNodeComponent;

/**
 * Comparator for {@link BabelNodeComponent} instances (specifically {@link IPBabelNodeComponent})
 * tailored for {@link pt.unl.fct.di.novasys.babel2.channels.DefaultNodeStore}.
 *
 * <p>Priority ordering:
 * <ol>
 *   <li>IPv6 addresses are prioritized over IPv4 addresses.</li>
 *   <li>Within both IPv6 and IPv4, local links (loopback, link-local) are prioritized before global addresses:
 *     <ul>
 *       <li>IPv6 Loopback ({@code ::1})</li>
 *       <li>IPv6 Link-Local ({@code fe80::/10})</li>
 *       <li>IPv6 Site-Local / ULA ({@code fc00::/7})</li>
 *       <li>IPv6 Global unicast</li>
 *       <li>IPv4 Loopback ({@code 127.0.0.0/8})</li>
 *       <li>IPv4 Link-Local ({@code 169.254.0.0/16})</li>
 *       <li>IPv4 Site-Local (RFC 1918 private: {@code 10.0.0.0/8}, {@code 172.16.0.0/12}, {@code 192.168.0.0/16})</li>
 *       <li>IPv4 Global public addresses</li>
 *     </ul>
 *   </li>
 *   <li>Within the same priority tier, tie-breaking is performed by comparing address bytes, port numbers,
 *       and channel names to guarantee strict consistency with {@code equals}.</li>
 * </ol>
 */
public class BabelIPComparator implements Comparator<BabelNodeComponent> {

    /**
     * Singleton instance of {@link BabelIPComparator}.
     */
    public static final BabelIPComparator INSTANCE = new BabelIPComparator();

    /**
     * Dedicated {@code Comparator<IPBabelNodeComponent>} delegate.
     */
    public static final Comparator<IPBabelNodeComponent> IP_COMPARATOR = INSTANCE::compare;

    public static final int IPV6_LOOPBACK = 0;
    public static final int IPV6_LINK_LOCAL = 1;
    public static final int IPV6_SITE_LOCAL = 2;
    public static final int IPV6_GLOBAL = 3;

    public static final int IPV4_LOOPBACK = 4;
    public static final int IPV4_LINK_LOCAL = 5;
    public static final int IPV4_SITE_LOCAL = 6;
    public static final int IPV4_GLOBAL = 7;
    public static final int UNRESOLVED_OR_UNKNOWN = 8;

    private static final ConcurrentMap<BabelNodeComponent, Integer> FALLBACK_IDS = new ConcurrentHashMap<>();
    private static final AtomicInteger FALLBACK_COUNTER = new AtomicInteger();

    public BabelIPComparator() {
    }

    @Override
    public int compare(BabelNodeComponent o1, BabelNodeComponent o2) {
        if (o1 == o2 || Objects.equals(o1, o2)) {
            return 0;
        }
        if (o1 == null) {
            return 1;
        }
        if (o2 == null) {
            return -1;
        }

        if (o1 instanceof IPBabelNodeComponent ip1 && o2 instanceof IPBabelNodeComponent ip2) {
            return compare(ip1, ip2);
        }

        if (o1 instanceof IPBabelNodeComponent) {
            return -1;
        }
        if (o2 instanceof IPBabelNodeComponent) {
            return 1;
        }

        int classCmp = o1.getClass().getName().compareTo(o2.getClass().getName());
        if (classCmp != 0) {
            return classCmp;
        }
        int hashCmp = Integer.compare(o1.hashCode(), o2.hashCode());
        if (hashCmp != 0) {
            return hashCmp;
        }
        int idCmp = Integer.compare(System.identityHashCode(o1), System.identityHashCode(o2));
        if (idCmp != 0) {
            return idCmp;
        }
        return Integer.compare(getFallbackId(o1), getFallbackId(o2));
    }

    /**
     * Compares two {@link IPBabelNodeComponent} instances according to the configured IP priorities.
     *
     * @param o1 the first component
     * @param o2 the second component
     * @return a negative integer, zero, or a positive integer as {@code o1} is higher priority (less than),
     *         equal to, or lower priority (greater than) {@code o2}
     */
    public int compare(IPBabelNodeComponent o1, IPBabelNodeComponent o2) {
        if (o1 == o2 || Objects.equals(o1, o2)) {
            return 0;
        }
        if (o1 == null) {
            return 1;
        }
        if (o2 == null) {
            return -1;
        }

        InetSocketAddress addr1 = o1.address();
        InetSocketAddress addr2 = o2.address();

        int prio1 = getAddressPriority(addr1);
        int prio2 = getAddressPriority(addr2);
        if (prio1 != prio2) {
            return Integer.compare(prio1, prio2);
        }

        InetAddress inet1 = addr1.getAddress();
        InetAddress inet2 = addr2.getAddress();
        if (inet1 != null && inet2 != null) {
            byte[] b1 = inet1.getAddress();
            byte[] b2 = inet2.getAddress();
            int minLen = Math.min(b1.length, b2.length);
            for (int i = 0; i < minLen; i++) {
                int cmp = Integer.compare(Byte.toUnsignedInt(b1[i]), Byte.toUnsignedInt(b2[i]));
                if (cmp != 0) {
                    return cmp;
                }
            }
            int lenCmp = Integer.compare(b1.length, b2.length);
            if (lenCmp != 0) {
                return lenCmp;
            }
            if (inet1 instanceof Inet6Address ip6_1 && inet2 instanceof Inet6Address ip6_2) {
                int scopeCmp = Integer.compare(ip6_1.getScopeId(), ip6_2.getScopeId());
                if (scopeCmp != 0) {
                    return scopeCmp;
                }
            }
        } else if (inet1 == null && inet2 != null) {
            return 1;
        } else if (inet1 != null && inet2 == null) {
            return -1;
        } else {
            int hostCmp = addr1.getHostString().compareTo(addr2.getHostString());
            if (hostCmp != 0) {
                return hostCmp;
            }
        }

        int portCmp = Integer.compare(addr1.getPort(), addr2.getPort());
        if (portCmp != 0) {
            return portCmp;
        }

        String chan1 = o1.channelName() != null ? o1.channelName() : "";
        String chan2 = o2.channelName() != null ? o2.channelName() : "";
        int chanCmp = chan1.compareTo(chan2);
        if (chanCmp != 0) {
            return chanCmp;
        }

        if (o1.equals(o2)) {
            return 0;
        }

        int idCmp = Integer.compare(System.identityHashCode(o1), System.identityHashCode(o2));
        if (idCmp != 0) {
            return idCmp;
        }
        return Integer.compare(getFallbackId(o1), getFallbackId(o2));
    }

    /**
     * Determines the priority bucket of a socket address.
     * Lower numbers correspond to higher priority.
     *
     * @param sockAddr the socket address
     * @return the priority tier
     */
    public static int getAddressPriority(InetSocketAddress sockAddr) {
        if (sockAddr == null) {
            return UNRESOLVED_OR_UNKNOWN;
        }
        InetAddress addr = sockAddr.getAddress();
        if (addr == null) {
            return UNRESOLVED_OR_UNKNOWN;
        }

        if (addr instanceof Inet6Address) {
            if (addr.isLoopbackAddress()) {
                return IPV6_LOOPBACK;
            }
            if (addr.isLinkLocalAddress()) {
                return IPV6_LINK_LOCAL;
            }
            if (addr.isSiteLocalAddress() || isIPv6ULA(addr)) {
                return IPV6_SITE_LOCAL;
            }
            return IPV6_GLOBAL;
        } else if (addr instanceof Inet4Address) {
            if (addr.isLoopbackAddress()) {
                return IPV4_LOOPBACK;
            }
            if (addr.isLinkLocalAddress()) {
                return IPV4_LINK_LOCAL;
            }
            if (addr.isSiteLocalAddress()) {
                return IPV4_SITE_LOCAL;
            }
            return IPV4_GLOBAL;
        }
        return UNRESOLVED_OR_UNKNOWN;
    }

    /**
     * Checks if the given IPv6 address is a Unique Local Address (ULA, fc00::/7).
     *
     * @param addr the address to check
     * @return {@code true} if the address is an IPv6 ULA, {@code false} otherwise
     */
    public static boolean isIPv6ULA(InetAddress addr) {
        if (addr instanceof Inet6Address) {
            byte[] bytes = addr.getAddress();
            return (Byte.toUnsignedInt(bytes[0]) & 0xfe) == 0xfc;
        }
        return false;
    }

    private static int getFallbackId(BabelNodeComponent c) {
        return FALLBACK_IDS.computeIfAbsent(c, k -> FALLBACK_COUNTER.incrementAndGet());
    }
}
