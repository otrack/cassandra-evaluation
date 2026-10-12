import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// Matrix <file> : store tasks (SafeTask) during deep backlog: protocol step x component, wall thread-seconds per backlog second
public class Matrix {
    static long ns(java.time.Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    static final String[][] RULES = {
        {"updateSize", "AccordCacheEntry.updateSize", "estimateHeapSize", "AccordObjectSizes."},
        {"journal", ".journal.", "AccordJournal", "CommandChangeWriter"},
        {"deps visit", "DepsCalculator", "CommandsForKey.visit", "SaferCommandStore.visit"},
        {"CFK update", "accord.local.cfk."},
        {"exec/read", "Txn.execute", "SinglePartitionReadCommand", "TxnRead", "TxnUpdate"},
        {"send", "org.apache.cassandra.net."},
        {"cache", "AccordCache", "AccordCacheEntry"},
    };
    public static void main(String[] a) throws Exception {
        long B = 10_000_000L; List<long[]> q = new ArrayList<>(); List<RecordedEvent> samples = new ArrayList<>(); long t0 = Long.MAX_VALUE, t1 = 0;
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); long s = ns(e.getStartTime()); t0 = Math.min(t0, s); t1 = Math.max(t1, s);
                String ty = e.getEventType().getName();
                if (ty.equals("profiler.Span") && Tags.isQueued(e.getString("tag"))) q.add(new long[]{s, ns(e.getEndTime())});
                else if (ty.equals("profiler.WallClockSample") && e.getString("state").contains("RUNNABLE")) {
                    RecordedThread t = e.getThread("sampledThread");
                    if (t != null && t.getJavaName() != null && t.getJavaName().startsWith("AccordExecutor")) samples.add(e);
                }
            }
        }
        long[] st = q.stream().mapToLong(x -> x[0]).sorted().toArray(), en = q.stream().mapToLong(x -> x[1]).sorted().toArray();
        Set<Long> deep = new HashSet<>();
        for (long b = t0 / B; b <= t1 / B; b++) { long mid = b * B + B / 2; if (Drain.up(st, mid) - Drain.up(en, mid) >= 50) deep.add(b); }
        double D = deep.size() * 0.01;
        Map<String, Map<String, Double>> m = new TreeMap<>(); Set<String> comps = new LinkedHashSet<>();
        for (String[] r : RULES) comps.add(r[0]); comps.add("other");
        for (RecordedEvent e : samples) {
            if (!deep.contains(ns(e.getStartTime()) / B) || e.getStackTrace() == null) continue;
            List<String> n = new ArrayList<>(); for (RecordedFrame f : e.getStackTrace().getFrames()) n.add(f.getMethod().getType().getName() + "." + f.getMethod().getName());
            int st0 = -1; String phase = null;
            for (int i = 0; i < n.size(); i++) { if (n.get(i).endsWith("SafeTask.runMayThrow")) { st0 = i; phase = "run"; break; } if (n.get(i).endsWith("SafeTask.completeExclusiveMayThrow")) { st0 = i; phase = "complete"; break; } if (n.get(i).endsWith("SafeTask.prepareExclusiveMayThrow")) { st0 = i; phase = "prepare"; break; } }
            if (st0 < 0) continue;
            String step = phase.equals("run") ? "run: (unclassified)" : phase;
            if (phase.equals("run")) for (int i = st0 - 1; i >= 0; i--) { String x = n.get(i);
                if (x.startsWith("accord.messages.") || x.matches("accord\\.local\\.(Commands|CommandStore|SafeCommandStore)\\.[a-z].*") || x.contains("ProgressLog") || x.contains("accord.coordinate.")) {
                    step = "run: " + x.replaceAll("^accord\\.", "").replaceAll("\\$.*", ""); break; } }
            String comp = "other";
            outer: for (String[] r : RULES) for (int k = 1; k < r.length; k++) for (int i = 0; i < st0; i++) if (n.get(i).contains(r[k])) { comp = r[0]; break outer; }
            m.computeIfAbsent(step, x -> new HashMap<>()).merge(comp, 0.010, Double::sum);
        }
        System.out.printf("== %s  (deep backlog %.1fs; thread-seconds per backlog-second, store tasks only)%n", Path.of(a[0]).getFileName(), D);
        System.out.printf("  %-44s", "step \\ component"); for (String c : comps) System.out.printf("%11s", c); System.out.printf("%9s%n", "TOTAL");
        Map<String, Double> colTot = new HashMap<>(); double grand = 0;
        List<Map.Entry<String, Map<String, Double>>> rows = new ArrayList<>(m.entrySet());
        rows.sort((x, y) -> Double.compare(y.getValue().values().stream().mapToDouble(d -> d).sum(), x.getValue().values().stream().mapToDouble(d -> d).sum()));
        for (var r : rows) {
            double rt = r.getValue().values().stream().mapToDouble(d -> d).sum(); if (rt / D < 0.005) continue;
            System.out.printf("  %-44s", r.getKey().length() > 44 ? r.getKey().substring(0, 44) : r.getKey());
            for (String c : comps) { double v = r.getValue().getOrDefault(c, 0.0) / D; colTot.merge(c, v, Double::sum); System.out.printf("%11s", v < 0.0005 ? "." : String.format("%.3f", v)); }
            grand += rt / D; System.out.printf("%9.3f%n", rt / D);
        }
        System.out.printf("  %-44s", "TOTAL"); for (String c : comps) System.out.printf("%11.3f", colTot.getOrDefault(c, 0.0)); System.out.printf("%9.3f%n", grand);
    }
}
