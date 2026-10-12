import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// Timeline <file> <bucketMs> [fromSec toSec]
// Per bucket: queued tasks (AccordTaskQueued spans open at bucket midpoint), executor threads parked in
// awaitExclusive (from batched wall samples), threads seen busy / waiting for the lock, stores run.
public class Timeline {
    static long ns(Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    static String cls(RecordedStackTrace st) {
        if (st == null) return "other";
        for (RecordedFrame f : st.getFrames()) {
            String m = f.getMethod().getType().getName() + "." + f.getMethod().getName();
            if (m.endsWith(".awaitExclusive")) return "idle";
            if (m.equals("java.util.concurrent.locks.ReentrantLock$Sync.lock")) return "lock";
            if (m.contains("AbstractLockLoop$")) return "busy";
        }
        return "busy";
    }
    public static void main(String[] a) throws Exception {
        Path p = Path.of(a[0]); long bucket = Long.parseLong(a[1]) * 1_000_000L;
        long t0 = Long.MAX_VALUE, t1 = 0;
        List<long[]> queued = new ArrayList<>();
        Map<String, List<long[]>> idle = new HashMap<>();          // thread -> idle intervals
        Map<Long, Map<String, Set<String>>> point = new HashMap<>(); // bucket -> class -> threads
        Map<Long, Map<String, Integer>> stores = new HashMap<>();
        Set<String> execThreads = new TreeSet<>();
        try (RecordingFile rf = new RecordingFile(p)) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                long s = ns(e.getStartTime()); t0 = Math.min(t0, s); t1 = Math.max(t1, s);
                String type = e.getEventType().getName();
                if (type.equals("profiler.Span")) {
                    String tag = e.getString("tag");
                    if (Tags.isQueued(tag)) queued.add(new long[]{s, ns(e.getEndTime())});
                    else if (Tags.isRun(tag) && Tags.isStoreTask(tag)) {
                        stores.computeIfAbsent(s / bucket, k -> new TreeMap<>()).merge(Tags.store(tag), 1, Integer::sum);
                    }
                } else if (type.equals("profiler.WallClockSample")) {
                    RecordedThread t = e.getThread("sampledThread");
                    String tn = t == null ? null : t.getJavaName();
                    if (tn == null || !tn.startsWith("AccordExecutor")) continue;
                    execThreads.add(tn);
                    String c = cls(e.getStackTrace());
                    long span = e.getDuration("timeSpan").toNanos();
                    if (c.equals("idle")) idle.computeIfAbsent(tn, k -> new ArrayList<>()).add(new long[]{s, s + Math.max(span, 1)});
                    else point.computeIfAbsent(s / bucket, k -> new HashMap<>()).computeIfAbsent(c, k -> new HashSet<>()).add(tn);
                }
            }
        }
        double from = a.length > 2 ? Double.parseDouble(a[2]) : 0, to = a.length > 3 ? Double.parseDouble(a[3]) : 1e9;
        // sweep
        long[] qStart = queued.stream().mapToLong(x -> x[0]).sorted().toArray();
        long[] qEnd = queued.stream().mapToLong(x -> x[1]).sorted().toArray();
        List<long[]> allIdle = new ArrayList<>(); idle.values().forEach(allIdle::addAll);
        long[] iStart = allIdle.stream().mapToLong(x -> x[0]).sorted().toArray();
        long[] iEnd = allIdle.stream().mapToLong(x -> x[1]).sorted().toArray();
        int n = execThreads.size();
        System.out.printf("executor threads=%d, queued spans=%d, buckets of %dms%n", n, queued.size(), bucket / 1_000_000);
        // aggregate: correlate queue depth with idle threads
        TreeMap<Integer, long[]> byDepth = new TreeMap<>(); // depthBand -> {buckets, sumIdle, sumBusy, sumLock}
        StringBuilder rows = new StringBuilder();
        for (long b = t0 / bucket; b <= t1 / bucket; b++) {
            long mid = b * bucket + bucket / 2;
            int q = countOpen(qStart, qEnd, mid), id = countOpen(iStart, iEnd, mid);
            Map<String, Set<String>> pt = point.getOrDefault(b, Map.of());
            int busy = pt.getOrDefault("busy", Set.of()).size(), lock = pt.getOrDefault("lock", Set.of()).size();
            int band = q == 0 ? 0 : q < 10 ? 1 : q < 50 ? 10 : q < 100 ? 50 : q < 200 ? 100 : 200;
            long[] acc = byDepth.computeIfAbsent(band, k -> new long[4]); acc[0]++; acc[1] += id; acc[2] += busy; acc[3] += lock;
            double rel = (mid - t0) / 1e9;
            if (rel >= from && rel <= to)
                rows.append(String.format("  +%8.3fs queued=%4d idle=%2d busy=%2d lockwait=%2d  stores=%s%n", rel, q, id, busy, lock, stores.getOrDefault(b, Map.of())));
        }
        System.out.println("queue depth band -> mean executor threads idle / seen busy / seen waiting for lock");
        byDepth.forEach((k, v) -> System.out.printf("  queued>=%-4d buckets=%6d  idle=%5.1f  busy=%5.1f  lockwait=%5.1f%n", k, v[0], (double) v[1] / v[0], (double) v[2] / v[0], (double) v[3] / v[0]));
        System.out.print(rows);
    }
    static int countOpen(long[] starts, long[] ends, long t) {
        return upper(starts, t) - upper(ends, t);
    }
    static int upper(long[] arr, long t) { int lo = 0, hi = arr.length; while (lo < hi) { int m = (lo + hi) >>> 1; if (arr[m] <= t) lo = m + 1; else hi = m; } return lo; }
}
