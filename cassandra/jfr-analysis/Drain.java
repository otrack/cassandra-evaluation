import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// Drain <file> : per 10ms bucket, queued depth at midpoint vs queued-span ends (= tasks dispatched) and distinct dispatching threads
public class Drain {
    static long ns(java.time.Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    public static void main(String[] a) throws Exception {
        long B = 10_000_000L; List<long[]> q = new ArrayList<>(); Map<Long, Set<String>> thr = new HashMap<>(); Map<Long, Integer> ends = new HashMap<>();
        long t0 = Long.MAX_VALUE, t1 = 0, cpu = 0;
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); long s = ns(e.getStartTime()); t0 = Math.min(t0, s); t1 = Math.max(t1, s);
                if (e.getEventType().getName().equals("jdk.ExecutionSample")) { RecordedThread t = e.getThread("sampledThread"); if (t != null && t.getJavaName() != null && t.getJavaName().startsWith("AccordExecutor")) cpu++; }
                if (!e.getEventType().getName().equals("profiler.Span") || !Tags.isQueued(e.getString("tag"))) continue;
                long en = ns(e.getEndTime()); q.add(new long[]{s, en});
                ends.merge(en / B, 1, Integer::sum); thr.computeIfAbsent(en / B, k -> new HashSet<>()).add(e.getThread().getJavaName());
            }
        }
        long[] st = q.stream().mapToLong(x -> x[0]).sorted().toArray(), en = q.stream().mapToLong(x -> x[1]).sorted().toArray();
        long deep = 0, deepEnds = 0, deepThr = 0, shallow = 0, shallowEnds = 0;
        for (long b = t0 / B; b <= t1 / B; b++) {
            long mid = b * B + B / 2; int open = up(st, mid) - up(en, mid);
            int e = ends.getOrDefault(b, 0), t = thr.getOrDefault(b, Set.of()).size();
            if (open >= 50) { deep++; deepEnds += e; deepThr += t; } else { shallow++; shallowEnds += e; }
        }
        double secs = (t1 - t0) / 1e9;
        System.out.printf("%s%n  duration %.0fs, executor CPU samples %.0f/s (x the effective ctimer interval = cores; ~4ms on HZ=250 kernels, not the configured 1ms)%n", Path.of(a[0]).getFileName(), secs, cpu / secs);
        System.out.printf("  deep-queue buckets (>=50 queued): %d (%.0f%% of run): dispatch rate %.0f tasks/s, distinct dispatching threads per 10ms %.1f%n", deep, 100.0 * deep / (deep + shallow), deepEnds / (deep * 0.01), (double) deepThr / deep);
        System.out.printf("  other buckets: dispatch rate %.0f tasks/s%n", shallowEnds / (shallow * 0.01));
    }
    static int up(long[] a, long t) { int lo = 0, hi = a.length; while (lo < hi) { int m = (lo + hi) >>> 1; if (a[m] <= t) lo = m + 1; else hi = m; } return lo; }
}
