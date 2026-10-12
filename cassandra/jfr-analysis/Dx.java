import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.function.*;
import java.util.regex.*;

/**
 * Shared parsing of the "DX ..." spans emitted by Cassandra's DebugDistributedExecution
 * (-Daccord.debug_distributed_execution[_keys]=N), for TxnTrace, LinkDelays and TxnLatency.
 *
 * <ul>
 *   <li>{@link #scan}: streams every DX span of the given recordings, with the "DX " prefix removed and the node
 *       named from the file name ({@code ..._<Node>.jfr}) or, for in-JVM dtests, the {@code nodeN_} thread prefix</li>
 *   <li>{@link Window}: the {@code --from HH:MM:SS[.sss] --to HH:MM:SS[.sss]} (UTC) and {@code --skip <seconds>}
 *       (after the first Client span) options shared by the tools</li>
 *   <li>{@link Messages}: one-way delays of messages, pairing {@code M Send} with {@code M Recv} and
 *       {@code M Reply} with {@code M Resp}</li>
 * </ul>
 * TxnIds are printed as {@code [epoch,hlc,flags(kind),node]}, e.g. {@code [28,1791462142555396,194(KW),1]}: KW is a
 * key write, KE a key ephemeral read, KR a key read, RX a range sync point. The last field is the coordinator's
 * node id, which is how node ids are mapped to node names (see {@link #nodeIds}).
 */
final class Dx
{
    private Dx() {}

    /** a DX span; tag has the "DX " prefix removed */
    record Ev(Instant start, long durNanos, String node, String thread, String tag)
    {
        Instant end() { return start.plusNanos(durNanos); }
    }

    static final Pattern TXN = Pattern.compile("\\[\\d+,\\d+,[^\\]]*\\]");
    static final Pattern TXN_PARTS = Pattern.compile("^\\[(\\d+),(\\d+),\\d+\\((\\w+)\\),(\\d+)\\]$");
    static final Pattern CLIENT = Pattern.compile("^C Client (\\[\\d+,\\d+,[^\\]]*\\])");
    static final Pattern EXECUTING = Pattern.compile("^E Executing (\\[\\d+,\\d+,[^\\]]*\\]) (\\w+)");
    static final Pattern MSG = Pattern.compile("^M (Send|Recv|Reply|Resp) (\\S+) (\\[\\d+,\\d+,[^\\]]*\\]) n(\\d+)$");
    /** a transaction's own update of a key: "K <SaveStatus> <txn> k<key> s<store>" (not Unblock/BlockedOn) */
    static final Pattern KEY_UPDATE = Pattern.compile("^K (\\S+) (\\[\\d+,\\d+,[^\\]]*\\]) k(\\S+) s(\\d+)$");

    static String fileNode(String file)
    {
        return Path.of(file).getFileName().toString().replaceAll("\\.jfr$", "").replaceAll("^.*_", "");
    }

    /** streams every DX span in the files, in file order (not time order) */
    static void scan(List<String> files, Consumer<Ev> sink) throws Exception
    {
        for (String f : files)
        {
            String fileNode = fileNode(f);
            try (RecordingFile rf = new RecordingFile(Path.of(f)))
            {
                while (rf.hasMoreEvents())
                {
                    RecordedEvent e = rf.readEvent();
                    if (!e.getEventType().getName().equals("profiler.Span")) continue;
                    String tag = e.getString("tag");
                    if (tag == null || !tag.startsWith("DX ")) continue;
                    String thread = e.getThread() == null ? "?" : String.valueOf(e.getThread().getJavaName());
                    String node = thread.matches("node\\d+_.*") ? thread.substring(0, thread.indexOf('_')) : fileNode;
                    sink.accept(new Ev(e.getStartTime(), e.getDuration().toNanos(), node, thread, tag.substring(3)));
                }
            }
        }
    }

    /** the kind of a TxnId (KW, KE, RX, ...), or "?" */
    static String kind(String txn)
    {
        Matcher m = TXN_PARTS.matcher(txn);
        return m.matches() ? m.group(3) : "?";
    }

    /** the id of the node that coordinated (created) the TxnId, or -1 */
    static int coordinator(String txn)
    {
        Matcher m = TXN_PARTS.matcher(txn);
        return m.matches() ? Integer.parseInt(m.group(4)) : -1;
    }

    /**
     * node id -> node name, from the coordinators of the Client spans seen on each node. A node that coordinated
     * nothing is named if it is an in-JVM dtest node ("nodeN" is id N), or if it is the only unnamed node and there
     * is only one unnamed id among the peers; otherwise its messages are reported as "n<id>".
     */
    static Map<Integer, String> nodeIds(Map<String, Integer> idByNode)
    {
        return nodeIds(idByNode, Set.of(), Set.of());
    }

    static Map<Integer, String> nodeIds(Map<String, Integer> idByNode, Set<String> nodes, Set<Integer> peerIds)
    {
        Map<Integer, String> names = new TreeMap<>();
        idByNode.forEach((node, id) -> names.put(id, node));
        List<String> unnamedNodes = new ArrayList<>();
        for (String node : nodes)
        {
            if (idByNode.containsKey(node)) continue;
            Matcher m = Pattern.compile("^node(\\d+)$").matcher(node);
            if (m.matches() && !names.containsKey(Integer.parseInt(m.group(1)))) names.put(Integer.parseInt(m.group(1)), node);
            else unnamedNodes.add(node);
        }
        List<Integer> unnamedIds = new ArrayList<>();
        for (int id : peerIds) if (!names.containsKey(id)) unnamedIds.add(id);
        if (unnamedNodes.size() == 1 && unnamedIds.size() == 1) names.put(unnamedIds.get(0), unnamedNodes.get(0));
        return names;
    }

    static double ms(long nanos) { return nanos / 1e6; }

    static String utc(Instant t) { return LocalTime.ofInstant(t, ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MILLIS).toString(); }

    /** --from/--to (UTC times of day, like WindowAbs) and --skip (seconds after the first Client span) */
    static final class Window
    {
        LocalTime from, to;
        double skipSeconds = -1;
        Instant skipFrom; // resolved by resolve()

        /** consumes a window option at args[i]; returns the number of args consumed (0 if not a window option) */
        int accept(String[] args, int i)
        {
            switch (args[i])
            {
                case "--from": from = LocalTime.parse(args[i + 1]); return 2;
                case "--to": to = LocalTime.parse(args[i + 1]); return 2;
                case "--skip": skipSeconds = Double.parseDouble(args[i + 1]); return 2;
                default: return 0;
            }
        }

        /** resolve --skip against the first Client span (or the first event, if there are no Client spans) */
        void resolve(Instant firstClient)
        {
            if (skipSeconds >= 0 && firstClient != null)
                skipFrom = firstClient.plusNanos((long) (skipSeconds * 1e9));
        }

        boolean contains(Instant t)
        {
            if (skipFrom != null && t.isBefore(skipFrom)) return false;
            LocalTime lt = (from == null && to == null) ? null : LocalTime.ofInstant(t, ZoneOffset.UTC);
            if (from != null && lt.isBefore(from)) return false;
            if (to != null && !lt.isBefore(to)) return false;
            return true;
        }

        boolean any() { return from != null || to != null || skipSeconds >= 0; }

        String describe()
        {
            if (!any()) return "whole recording";
            List<String> s = new ArrayList<>();
            if (skipFrom != null) s.add("from " + utc(skipFrom) + " (" + skipSeconds + "s after the first Client span)");
            if (from != null) s.add("from " + from);
            if (to != null) s.add("to " + to);
            return String.join(", ", s) + " UTC";
        }

        static String usage() { return "[--from HH:MM:SS[.sss]] [--to HH:MM:SS[.sss]] [--skip <seconds after the first Client span>]"; }
    }

    /**
     * One-way message delays. Requests pair "M Send" (sender) with "M Recv" (receiver); responses pair "M Reply"
     * (replier) with "M Resp" (requester). Both are keyed by message type, TxnId and direction; repeated messages
     * with the same key (retries, or one request per command store) are paired in time order, i-th with i-th, and
     * any surplus on either side is counted as unpaired. A delay is from the sender's AccordMessageSink to the
     * receiver's verb handler: Cassandra's outbound queue, serialisation, the network and the receiver's event loop.
     * Local deliveries have no M events. Only as accurate as the hosts' clock sync.
     */
    static final class Messages
    {
        record Delay(String txn, String type, String from, String to, Instant sent, long nanos) {}

        // key: type|txn|fromId|toId -> times (epoch nanos)
        private final Map<String, long[]> sends = new HashMap<>(), recvs = new HashMap<>();
        private final Map<String, Integer> idByNode = new HashMap<>();
        private final Set<String> nodes = new HashSet<>();
        private final Set<Integer> peerIds = new HashSet<>();

        /** feed every DX event; returns true if it was a message event */
        boolean accept(Ev ev)
        {
            nodes.add(ev.node());
            Matcher c = CLIENT.matcher(ev.tag());
            if (c.find()) { idByNode.putIfAbsent(ev.node(), coordinator(c.group(1))); return false; }
            Matcher m = MSG.matcher(ev.tag());
            if (!m.matches()) return false;
            String type = m.group(2).replaceAll("_(REQ|RSP)$", ""), txn = m.group(3), peer = m.group(4);
            peerIds.add(Integer.parseInt(peer));
            long t = ev.start().getEpochSecond() * 1_000_000_000L + ev.start().getNano();
            boolean outbound = m.group(1).equals("Send") || m.group(1).equals("Reply");
            boolean response = m.group(1).equals("Reply") || m.group(1).equals("Resp");
            String dir = response ? "P" : "Q";
            // the local node's id is not known yet: key by name, resolved in delays()
            String key = outbound ? dir + "|" + type + "|" + txn + "|@" + ev.node() + "|" + peer
                                  : dir + "|" + type + "|" + txn + "|" + peer + "|@" + ev.node();
            add(outbound ? sends : recvs, key, t);
            return true;
        }

        private static void add(Map<String, long[]> map, String key, long t)
        {
            long[] v = map.get(key);
            if (v == null) { map.put(key, new long[]{ 1, t }); return; }
            int n = (int) v[0];
            if (n + 1 >= v.length) v = Arrays.copyOf(v, v.length * 2);
            v[n + 1] = t; v[0] = n + 1;
            map.put(key, v);
        }

        Map<Integer, String> names() { return nodeIds(idByNode, nodes, peerIds); }

        long unpaired;

        /** pairs sends with receives; call once, after every event has been accepted */
        List<Delay> delays()
        {
            Map<Integer, String> names = names();
            // normalise both sides to "dir|type|txn|fromName|toName"
            Map<String, long[]> s = normalise(sends, names), r = normalise(recvs, names);
            List<Delay> out = new ArrayList<>();
            for (var en : s.entrySet())
            {
                long[] a = en.getValue(), b = r.get(en.getKey());
                int na = (int) a[0], nb = b == null ? 0 : (int) b[0];
                long[] as = Arrays.copyOfRange(a, 1, na + 1);
                Arrays.sort(as);
                if (nb == 0) { unpaired += na; continue; }
                long[] bs = Arrays.copyOfRange(b, 1, nb + 1);
                Arrays.sort(bs);
                String[] k = en.getKey().split("\\|");
                int n = Math.min(na, nb);
                unpaired += Math.abs(na - nb);
                for (int i = 0; i < n; i++)
                {
                    long d = bs[i] - as[i];
                    if (d < 0) { unpaired += 2; continue; } // a mismatched pair
                    out.add(new Delay(k[2], k[1], k[3], k[4], Instant.ofEpochSecond(0, as[i]), d));
                }
            }
            for (var en : r.entrySet()) if (!s.containsKey(en.getKey())) unpaired += en.getValue()[0];
            return out;
        }

        private static Map<String, long[]> normalise(Map<String, long[]> in, Map<Integer, String> names)
        {
            Map<String, long[]> out = new HashMap<>(in.size() * 2);
            for (var en : in.entrySet())
            {
                String[] k = en.getKey().split("\\|");
                String key = k[0] + "|" + k[1] + "|" + k[2] + "|" + name(k[3], names) + "|" + name(k[4], names);
                long[] prev = out.putIfAbsent(key, en.getValue());
                if (prev != null)
                {
                    long[] v = en.getValue();
                    for (int i = 1; i <= v[0]; i++) add(out, key, v[i]);
                }
            }
            return out;
        }

        private static String name(String k, Map<Integer, String> names)
        {
            if (k.startsWith("@")) return k.substring(1);
            return names.getOrDefault(Integer.parseInt(k), "n" + k);
        }
    }

    static long percentile(long[] sorted, double p)
    {
        if (sorted.length == 0) return 0;
        return sorted[Math.min(sorted.length - 1, (int) (sorted.length * p))];
    }
}
