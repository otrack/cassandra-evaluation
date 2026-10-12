import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// NetEvents [--bucket ms] [--links regex] [--top N] [--from ..] [--to ..] <jfr>... : the JFR diagnostic events of
// -Dcassandra.jfr.diagnostic_events=true (recorded through async-profiler's jfrsync with cassandra/diagnostics.jfc).
//   - per outbound connection (sender -> receiver, type): messages and MB sent, the largest queue (pending), the
//     largest amount handed to Netty but not flushed (flushing), the share of samples it was unwritable, backpressure
//     intervals (count, total, longest), and TCP_INFO: cwnd (min/median), RTT (median/max), retransmits, the share of
//     samples in a loss state (CA Recovery/Loss), and the socket send buffer.
//   - --bucket ms: per connection over time: pending / unwritable% / cwnd / unacked / RTT ms / retransmits / KB sent
//   - per node: TCP counter deltas (retransmitted segments, RTO timeouts, loss probes, fast retransmits), the largest
//     Accord executor queue (waitingToRun), GC pauses, container CPU throttling; pressure stall time (PSI) of the
//     container for CPU, memory and I/O, major faults, swap-ins, and safepoints / VM operations of 50 ms or more
//     (with --bucket also a per-node PSI timeline). Simultaneous long safepoints on every node with PSI memory/io stall
//     time point at the host (page faults on memory mapped after mlockall: journal/SSTable mmaps, later allocations).
//   - --top N (default 10): the longest backpressure intervals
// How to read it: a queue (pending) with the connection unwritable means the socket was not draining. Then, if
// unacked ~ cwnd and cwnd collapsed with retransmits (CA state Recovery/Loss), TCP was recovering from loss; if
// unacked < cwnd, the send buffer or the receiver's window was the limit (compare soSndBuf, and the receiver's
// Inbound tcpRcvSpace). A queue while writable means the event loop was not getting to it (CPU, or a stall).
// Node names come from the file names; peers are named by matching their address with another file's local address.
public class NetEvents
{
    record Sample(Instant t, RecordedEvent e) {}

    public static void main(String[] args) throws Exception
    {
        long bucketMs = 0; int top = 10; String linkRe = null; Dx.Window window = new Dx.Window(); List<String> files = new ArrayList<>();
        for (int i = 0; i < args.length; )
        {
            int n = window.accept(args, i);
            if (n > 0) { i += n; continue; }
            switch (args[i])
            {
                case "--bucket": bucketMs = Long.parseLong(args[i + 1]); i += 2; break;
                case "--links": linkRe = args[i + 1]; i += 2; break;
                case "--top": top = Integer.parseInt(args[i + 1]); i += 2; break;
                default: files.add(args[i++]);
            }
        }
        if (files.isEmpty())
        {
            System.err.println("usage: NetEvents [--bucket ms] [--links regex] [--top N] [--from HH:MM:SS] [--to HH:MM:SS] <jfr>...");
            System.exit(2);
        }

        Map<String, String> nameOfAddress = new HashMap<>();
        Map<String, List<Sample>> outbound = new TreeMap<>(), tcp = new TreeMap<>(), executors = new TreeMap<>(), pressure = new TreeMap<>();
        Map<String, List<RecordedEvent>> safepoints = new TreeMap<>();
        Map<String, List<Sample>> kthreads = new TreeMap<>();
        List<RecordedEvent> backpressure = new ArrayList<>();
        Map<String, long[]> gc = new TreeMap<>();           // node -> [pauses, total nanos, max nanos]
        Map<String, List<RecordedEvent>> throttling = new TreeMap<>();
        Map<RecordedEvent, String> nodeOf = new IdentityHashMap<>();
        Instant first = null;
        int seen = 0;

        for (String f : files)
        {
            String fileNode = Dx.fileNode(f);
            boolean dtest = false;
            try (RecordingFile rf = new RecordingFile(Path.of(f)))
            {
                while (rf.hasMoreEvents())
                {
                    RecordedEvent e = rf.readEvent();
                    String type = e.getEventType().getName();
                    if (!type.startsWith("cassandra.") && !type.equals("jdk.GCPhasePause") && !type.equals("jdk.ContainerCPUThrottling")
                        && !type.equals("jdk.SafepointBegin") && !type.equals("jdk.ExecuteVMOperation")) continue;
                    if (first == null || e.getStartTime().isBefore(first)) first = e.getStartTime();
                    String local = e.hasField("local") ? e.getString("local") : null;
                    // in-JVM dtests: several nodes per recording, told apart by their local address
                    String node = files.size() == 1 && local != null ? local : fileNode;
                    if (local != null) nameOfAddress.putIfAbsent(local, node);
                    seen++;
                    switch (type)
                    {
                        case "cassandra.net.Outbound":
                            outbound.computeIfAbsent(node + "|" + e.getString("peer") + "|" + e.getString("type"), k -> new ArrayList<>()).add(new Sample(e.getStartTime(), e));
                            break;
                        case "cassandra.net.Backpressure": backpressure.add(e); nodeOf.put(e, node); break;
                        case "cassandra.os.Tcp": tcp.computeIfAbsent(node, k -> new ArrayList<>()).add(new Sample(e.getStartTime(), e)); break;
                        case "cassandra.accord.Executor": executors.computeIfAbsent(node, k -> new ArrayList<>()).add(new Sample(e.getStartTime(), e)); break;
                        case "cassandra.os.Pressure": pressure.computeIfAbsent(node, k -> new ArrayList<>()).add(new Sample(e.getStartTime(), e)); break;
                        case "cassandra.os.Threads": kthreads.computeIfAbsent(node, k -> new ArrayList<>()).add(new Sample(e.getStartTime(), e)); break;
                        case "jdk.SafepointBegin": case "jdk.ExecuteVMOperation":
                            if (e.getDuration().toMillis() >= 50) safepoints.computeIfAbsent(fileNode, k -> new ArrayList<>()).add(e);
                            break;
                        case "jdk.GCPhasePause":
                        {
                            long d = e.getDuration().toNanos(); long[] g = gc.computeIfAbsent(fileNode, k -> new long[3]);
                            g[0]++; g[1] += d; g[2] = Math.max(g[2], d); break;
                        }
                        case "jdk.ContainerCPUThrottling": throttling.computeIfAbsent(fileNode, k -> new ArrayList<>()).add(e); break;
                        default:
                    }
                }
            }
        }
        if (seen == 0)
        {
            System.out.println("no cassandra.* JFR events: was the node started with -Dcassandra.jfr.diagnostic_events=true, and the capture taken with jfrsync=<diagnostics.jfc>?");
            return;
        }
        window.resolve(first);
        System.out.printf("%d recordings; %s%n", files.size(), window.describe());

        // -- per outbound connection
        System.out.printf("%n%-40s %8s %8s %8s %9s %6s %13s %11s %11s %7s %6s %8s%n", "outbound connection", "msgs", "MB", "pendMax", "flushMax",
                          "unw%", "bp n/tot/max", "cwnd mn/md", "rtt md/mx", "retrans", "loss%", "sndbuf");
        Map<String, long[]> bpByConn = new HashMap<>(); // n, total, max (nanos)
        for (RecordedEvent e : backpressure)
        {
            if (!window.contains(e.getStartTime())) continue;
            String key = nodeOf.get(e) + "|" + e.getString("peer") + "|" + e.getString("type");
            long d = e.getDuration().toNanos(); long[] v = bpByConn.computeIfAbsent(key, k -> new long[3]);
            v[0]++; v[1] += d; v[2] = Math.max(v[2], d);
        }
        for (var en : outbound.entrySet())
        {
            List<Sample> l = new ArrayList<>(en.getValue());
            l.removeIf(s -> !window.contains(s.t()));
            if (l.size() < 2) continue;
            String label = label(en.getKey(), nameOfAddress);
            if (linkRe != null && !label.matches(".*(" + linkRe + ").*")) continue;
            RecordedEvent a = l.get(0).e(), z = l.get(l.size() - 1).e();
            long msgs = z.getLong("sentCount") - a.getLong("sentCount"), bytes = z.getLong("sentBytes") - a.getLong("sentBytes");
            if (msgs == 0 && l.stream().allMatch(s -> s.e().getInt("pendingCount") == 0)) continue;
            int pendMax = 0, unwritable = 0, loss = 0, tcpSamples = 0; long flushMax = 0, rttMax = -1, sndBuf = -1;
            List<Long> cwnd = new ArrayList<>(), rtt = new ArrayList<>();
            for (Sample s : l)
            {
                RecordedEvent e = s.e();
                pendMax = Math.max(pendMax, e.getInt("pendingCount"));
                flushMax = Math.max(flushMax, e.getLong("flushingBytes"));
                if (!e.getBoolean("writable")) unwritable++;
                if (e.getLong("tcpSndCwnd") >= 0)
                {
                    tcpSamples++;
                    cwnd.add(e.getLong("tcpSndCwnd")); rtt.add(e.getLong("tcpRtt"));
                    rttMax = Math.max(rttMax, e.getLong("tcpRtt"));
                    int ca = e.getInt("tcpCaState"); if (ca >= 3) loss++;
                    sndBuf = Math.max(sndBuf, e.getLong("soSndBuf"));
                }
            }
            long retrans = z.getLong("tcpTotalRetrans") >= 0 && a.getLong("tcpTotalRetrans") >= 0 ? z.getLong("tcpTotalRetrans") - a.getLong("tcpTotalRetrans") : -1;
            Collections.sort(cwnd); Collections.sort(rtt);
            long[] bp = bpByConn.getOrDefault(en.getKey(), new long[3]);
            System.out.printf("%-40s %8d %8.1f %8d %9s %5.1f%% %13s %11s %11s %7s %5s %8s%n", label, msgs, bytes / 1e6, pendMax, kb(flushMax),
                              100.0 * unwritable / l.size(), String.format("%d/%.0f/%.0f", bp[0], bp[1] / 1e6, bp[2] / 1e6),
                              cwnd.isEmpty() ? "-" : cwnd.get(0) + "/" + cwnd.get(cwnd.size() / 2),
                              rtt.isEmpty() ? "-" : String.format("%.0f/%.0f", rtt.get(rtt.size() / 2) / 1e3, rttMax / 1e3),
                              retrans < 0 ? "-" : String.valueOf(retrans), tcpSamples == 0 ? "-" : String.format("%.0f%%", 100.0 * loss / tcpSamples), kb(sndBuf));
        }
        System.out.println("  (MB, pending etc. over the window; bp = backpressure intervals n/total ms/max ms; rtt in ms; loss% = samples in CA Recovery/Loss)");

        if (top > 0 && !backpressure.isEmpty())
        {
            List<RecordedEvent> l = new ArrayList<>(backpressure);
            l.removeIf(e -> !window.contains(e.getStartTime()));
            l.sort((x, y) -> y.getDuration().compareTo(x.getDuration()));
            System.out.printf("%nlongest backpressure intervals:%n  %-12s %9s  %-40s %8s %8s %6s %6s %8s %8s%n", "start UTC", "ms", "connection", "pendStart", "pendEnd", "cwnd", "unack", "rtt ms", "retrans");
            for (RecordedEvent e : l.subList(0, Math.min(top, l.size())))
                System.out.printf("  %-12s %9.1f  %-40s %8d %8d %6d %6d %8.1f %8d%n", Dx.utc(e.getStartTime()), e.getDuration().toNanos() / 1e6,
                                  label(nodeOf.get(e) + "|" + e.getString("peer") + "|" + e.getString("type"), nameOfAddress),
                                  e.getInt("pendingCountAtStart"), e.getInt("pendingCountAtEnd"), e.getLong("tcpSndCwnd"), e.getLong("tcpUnacked"),
                                  e.getLong("tcpRtt") / 1e3, e.getLong("tcpTotalRetrans"));
        }

        // -- per node
        System.out.printf("%n%-14s %10s %9s %9s %9s %9s %9s %10s %12s %14s%n", "node", "outSegs", "retrans", "rto", "lossProbe", "fastRetr", "rcvQDrop",
                          "execWaitMx", "gc n/max ms", "throttled ms");
        Set<String> nodes = new TreeSet<>(); nodes.addAll(tcp.keySet()); nodes.addAll(executors.keySet()); nodes.addAll(gc.keySet());
        for (String node : nodes)
        {
            List<Sample> t = new ArrayList<>(tcp.getOrDefault(node, List.of()));
            t.removeIf(s -> !window.contains(s.t()));
            String[] d = { "-", "-", "-", "-", "-", "-" };
            if (t.size() >= 2)
            {
                RecordedEvent a = t.get(0).e(), z = t.get(t.size() - 1).e();
                String[] f = { "outSegs", "retransSegs", "tcpTimeouts", "tcpLossProbes", "tcpFastRetrans", "tcpRcvQDrop" };
                for (int i = 0; i < f.length; i++) d[i] = String.valueOf(z.getLong(f[i]) - a.getLong(f[i]));
            }
            int waitMax = -1;
            for (Sample s : executors.getOrDefault(node, List.of())) if (window.contains(s.t())) waitMax = Math.max(waitMax, s.e().getInt("waitingToRun"));
            long[] g = gc.get(node);
            String thr = "-";
            List<RecordedEvent> th = throttling.getOrDefault(node, List.of());
            if (th.size() >= 2 && th.get(0).hasField("cpuThrottledTime"))
            {
                RecordedEvent a = th.get(0), z = th.get(th.size() - 1);
                thr = String.format("%.0f (%d)", (z.getDuration("cpuThrottledTime").toNanos() - a.getDuration("cpuThrottledTime").toNanos()) / 1e6,
                                    z.getLong("cpuThrottledSlices") - a.getLong("cpuThrottledSlices"));
            }
            System.out.printf("%-14s %10s %9s %9s %9s %9s %9s %10s %12s %14s%n", node, d[0], d[1], d[2], d[3], d[4], d[5], waitMax < 0 ? "-" : String.valueOf(waitMax),
                              g == null ? "-" : String.format("%d/%.1f", g[0], g[2] / 1e6), thr);
        }
        System.out.println("  (TCP deltas over the window from cassandra.os.Tcp; gc = jdk.GCPhasePause over the recording; throttled = container CPU throttled time (periods))");

        System.out.printf("%n%-14s %10s %10s %10s %10s %10s %9s %9s %9s %8s %9s %9s %5s  %s%n", "node", "cpuSome", "memSome", "memFull", "ioSome", "ioFull", "majflt", "hostMaj", "swapIn", "memMax",
                          "steal", "cpuStlMx", "Dmax", "slow safepoints/VM ops >= 50ms (start ms)");
        for (String node : nodes)
        {
            List<Sample> p = new ArrayList<>(pressure.getOrDefault(node, List.of()));
            p.removeIf(s -> !window.contains(s.t()));
            String[] f = { "cpuSome", "memorySome", "memoryFull", "ioSome", "ioFull", "cgroupMajorFaults", "hostMajorFaults", "hostSwapIn", "memoryMax" };
            String[] d = new String[f.length]; Arrays.fill(d, "-");
            if (p.size() >= 2)
            {
                RecordedEvent a = p.get(0).e(), z = p.get(p.size() - 1).e();
                for (int i = 0; i < f.length; i++)
                {
                    long x = rawLong(a, f[i]), y = rawLong(z, f[i]);
                    if (x >= 0 && y >= 0) d[i] = i < 5 ? String.format("%.0fms", (y - x) / 1e3) : String.valueOf(y - x);
                }
            }
            String steal = "-", stealMax = "-", dMax = "-";
            if (p.size() >= 2 && rawLong(p.get(0).e(), "hostSteal") >= 0)
            {
                // hostSteal is a millisecond Timespan: rawLong returns microseconds
                steal = String.format("%.0fms", (rawLong(p.get(p.size() - 1).e(), "hostSteal") - rawLong(p.get(0).e(), "hostSteal")) / 1e3);
                long mx = 0;
                for (Sample x : p) mx = Math.max(mx, rawLong(x.e(), "hostMaxCpuSteal"));
                stealMax = String.format("%.0fms", mx / 1e3);
            }
            List<Sample> kt = kthreads.getOrDefault(node, List.of());
            if (!kt.isEmpty())
            {
                int mx = 0;
                for (Sample x : kt) if (window.contains(x.t())) mx = Math.max(mx, x.e().getInt("uninterruptible"));
                dMax = String.valueOf(mx);
            }
            StringBuilder sp = new StringBuilder();
            for (RecordedEvent e : safepoints.getOrDefault(node, List.of()))
                if (window.contains(e.getStartTime()))
                    sp.append(String.format(" %s(%s %.0f)", e.getEventType().getName().endsWith("SafepointBegin") ? "safepoint" : e.getString("operation"), Dx.utc(e.getStartTime()), e.getDuration().toNanos() / 1e6));
            System.out.printf("%-14s %10s %10s %10s %10s %10s %9s %9s %9s %8s %9s %9s %5s %s%n", node, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], steal, stealMax, dMax, sp);
        }
        System.out.println("  (cassandra.os.Pressure deltas: PSI stall time of the container's cgroup, its major faults and memory.max events; host major faults, swap-ins,");
        System.out.println("   CPU steal (all CPUs; cpuStlMx = most of one CPU in one period); Dmax = most JVM threads in uninterruptible sleep at once (cassandra.os.Threads))");

        if (bucketMs <= 0) return;
        System.out.printf("%nper %dms bucket from %s UTC, per node: PSI ms stalled in the bucket (memSome/ioSome/cpuSome) and major faults%n", bucketMs, Dx.utc(first));
        for (var en : pressure.entrySet())
        {
            TreeMap<Long, List<RecordedEvent>> b = new TreeMap<>();
            for (Sample s : en.getValue()) if (window.contains(s.t())) b.computeIfAbsent(Duration.between(first, s.t()).toMillis() / bucketMs, k -> new ArrayList<>()).add(s.e());
            StringBuilder sb = new StringBuilder(String.format("%-14s", en.getKey()));
            RecordedEvent prev = null;
            for (var bb : b.entrySet())
            {
                RecordedEvent last = bb.getValue().get(bb.getValue().size() - 1);
                if (prev != null)
                    sb.append(String.format(" %s/%s/%s/%s", deltaMs(prev, last, "memorySome"), deltaMs(prev, last, "ioSome"), deltaMs(prev, last, "cpuSome"),
                                            rawLong(prev, "cgroupMajorFaults") < 0 ? "-" : String.valueOf(rawLong(last, "cgroupMajorFaults") - rawLong(prev, "cgroupMajorFaults"))));
                prev = last;
            }
            System.out.println(sb);
        }

        System.out.printf("%nper %dms bucket from %s UTC, per connection: pendingMax/unwritable%%/cwnd/unacked/rttMs/retrans/KBsent%n", bucketMs, Dx.utc(first));
        for (var en : outbound.entrySet())
        {
            String label = label(en.getKey(), nameOfAddress);
            if (linkRe != null && !label.matches(".*(" + linkRe + ").*")) continue;
            TreeMap<Long, List<RecordedEvent>> b = new TreeMap<>();
            for (Sample s : en.getValue()) if (window.contains(s.t())) b.computeIfAbsent(Duration.between(first, s.t()).toMillis() / bucketMs, k -> new ArrayList<>()).add(s.e());
            if (b.isEmpty()) continue;
            boolean active = b.values().stream().flatMap(List::stream).anyMatch(e -> e.getInt("pendingCount") > 0 || !e.getBoolean("writable"));
            long sent = b.lastEntry().getValue().get(b.lastEntry().getValue().size() - 1).getLong("sentBytes") - b.firstEntry().getValue().get(0).getLong("sentBytes");
            if (!active && sent == 0) continue;
            StringBuilder sb = new StringBuilder(String.format("%-40s", label));
            long prevSent = -1, prevRetrans = -1;
            for (var bb : b.entrySet())
            {
                List<RecordedEvent> l = bb.getValue();
                RecordedEvent last = l.get(l.size() - 1);
                int pend = 0, unw = 0;
                for (RecordedEvent e : l) { pend = Math.max(pend, e.getInt("pendingCount")); if (!e.getBoolean("writable")) unw++; }
                long sentNow = last.getLong("sentBytes"), retransNow = last.getLong("tcpTotalRetrans");
                boolean hasTcp = last.getLong("tcpSndCwnd") >= 0;
                sb.append(String.format(" %d/%d/%s/%s/%s/%s/%s", pend, 100 * unw / l.size(), hasTcp ? String.valueOf(last.getLong("tcpSndCwnd")) : "-",
                                        hasTcp ? String.valueOf(last.getLong("tcpUnacked")) : "-", hasTcp ? String.format("%.0f", last.getLong("tcpRtt") / 1e3) : "-",
                                        prevRetrans < 0 || retransNow < 0 ? "-" : String.valueOf(retransNow - prevRetrans), prevSent < 0 ? "-" : String.valueOf((sentNow - prevSent) / 1024)));
                prevSent = sentNow; prevRetrans = retransNow;
            }
            System.out.println(sb);
        }
    }

    /** a long field's raw value (Timespan fields included), or -1 */
    private static long rawLong(RecordedEvent e, String field)
    {
        if (!e.hasField(field)) return -1;
        Object v = e.getValue(field);
        if (v instanceof Duration) return ((Duration) v).toNanos() / 1000; // microseconds, as recorded
        return v instanceof Number ? ((Number) v).longValue() : -1;
    }

    private static String deltaMs(RecordedEvent a, RecordedEvent b, String field)
    {
        long x = rawLong(a, field), y = rawLong(b, field);
        return x < 0 || y < 0 ? "-" : String.format("%.0f", (y - x) / 1e3);
    }

    private static String label(String key, Map<String, String> nameOfAddress)
    {
        String[] k = key.split("\\|");
        String type = k[2].replace("_MESSAGES", "").toLowerCase();
        return k[0] + "->" + nameOfAddress.getOrDefault(k[1], k[1]) + " " + type;
    }

    private static String kb(long bytes)
    {
        return bytes < 0 ? "-" : bytes < 1024 ? bytes + "B" : String.format("%.0fK", bytes / 1024.0);
    }
}
