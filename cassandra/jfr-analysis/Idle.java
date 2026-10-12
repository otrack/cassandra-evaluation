import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// Idle <file> : investigate deep-backlog ticks in which no executor thread is RUNNABLE.
public class Idle {
    static long ns(java.time.Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    record W(long s, long e, String thread, boolean runnable, String sig, long n) {}
    static String sig(List<String> n) {
        for (String x : n) {
            if (x.endsWith("awaitExclusive")) return "parked: awaitExclusive (idle, waiting for hasWork)";
            if (x.contains("SafepointSynchronize::block")) return "blocked at safepoint";
            if (x.contains("alloc_page_stall") || x.contains("ZPageAllocator::alloc_page_stall") || x.contains("ZPageAllocator::stall")) return "ZGC allocation stall";
            if (x.endsWith("ReentrantLock$Sync.lock") || x.endsWith("AccordExecutor.lock")) return "waiting for executor lock";
        }
        for (String x : n) if (x.startsWith("org.apache.cassandra.") || x.startsWith("accord.")) return "other: " + x.replaceAll(".*\\.([^.]+\\.[^.]+)$", "$1");
        return "other: " + (n.isEmpty() ? "-" : n.get(0));
    }
    static String top(List<String> n, int k) {
        List<String> o = new ArrayList<>();
        for (String x : n) { if (x.startsWith("libc.so") || x.startsWith("/usr/lib") || x.startsWith("./usr") || x.startsWith("libasyncProfiler") || x.contains("signalHandler")) continue; o.add(x.replaceAll("^libjvm\\.so\\.", "").replaceAll(".*\\.([^.]+\\.[^.]+)$", "$1")); if (o.size() >= k) break; }
        return String.join(" <- ", o);
    }
    public static void main(String[] a) throws Exception {
        List<long[]> q = new ArrayList<>(); List<W> exec = new ArrayList<>(); List<Long> times = new ArrayList<>();
        Map<Long, List<String>> vm = new TreeMap<>(); // time -> VM/GC thread running stacks
        List<long[]> anyRun = new ArrayList<>(); Map<Long, String> anyRunWho = new HashMap<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); String ty = e.getEventType().getName();
                if (ty.equals("profiler.Span") && Tags.isQueued(e.getString("tag"))) { q.add(new long[]{ns(e.getStartTime()), ns(e.getEndTime())}); continue; }
                if (!ty.equals("profiler.WallClockSample")) continue;
                long s = ns(e.getStartTime()), n = e.getLong("samples");
                if (n == 1) times.add(s);
                RecordedThread th = e.getThread("sampledThread"); String tn = th == null ? "?" : th.getJavaName() != null ? th.getJavaName() : th.getOSName();
                boolean run = e.getString("state").contains("RUNNABLE");
                List<String> fr = new ArrayList<>(); if (e.getStackTrace() != null) for (RecordedFrame f : e.getStackTrace().getFrames()) fr.add(f.getMethod().getType().getName().isEmpty() ? f.getMethod().getName() : f.getMethod().getType().getName() + "." + f.getMethod().getName());
                if (tn.startsWith("AccordExecutor")) exec.add(new W(s, s + e.getDuration("timeSpan").toNanos(), tn, run, sig(fr), n));
                else if (run && (tn.startsWith("VM Thread") || tn.startsWith("Z") || tn.startsWith("GC"))) vm.computeIfAbsent(s, k -> new ArrayList<>()).add(tn + ": " + top(fr, 4));
                else if (run && n == 1) { anyRun.add(new long[]{s}); anyRunWho.put(s, tn.replaceAll("\\d+", "N") + ": " + top(fr, 3)); }
            }
        }
        Collections.sort(times); List<Long> tick = new ArrayList<>(); long last = -1;
        for (long t : times) { if (last < 0 || t - last > 3_000_000L) tick.add(t); last = t; }
        long[] T = tick.stream().mapToLong(x -> x).toArray();
        long[] st = q.stream().mapToLong(x -> x[0]).sorted().toArray(), en = q.stream().mapToLong(x -> x[1]).sorted().toArray();
        // per tick: executor RUNNABLE present?
        boolean[] run = new boolean[T.length];
        for (W w : exec) if (w.runnable() && w.n() == 1) { int i = Arrays.binarySearch(T, w.s()); if (i < 0) i = -i - 2; if (i >= 0) run[i] = true; }
        // state of every executor thread at a tick: an un-batched sample within [tick, tick+3ms) or a batched event covering the tick
        exec.sort(Comparator.comparingLong(W::s));
        Map<String, Long> sigCount = new TreeMap<>(); long idleTicks = 0, threadsSeen = 0; Map<String, Long> vmAt = new HashMap<>(); long vmTicks = 0;
        List<Integer> runs = new ArrayList<>(); int cur = 0; Map<String, Long> resumer = new HashMap<>();
        TreeMap<Long, String> vmMap = new TreeMap<>(); vm.forEach((k, v) -> vmMap.put(k, String.join(" | ", v)));
        long[] AR = anyRun.stream().mapToLong(x -> x[0]).sorted().toArray();
        for (int i = 0; i < T.length; i++) {
            boolean deep = Drain.up(st, T[i]) - Drain.up(en, T[i]) >= 50;
            boolean idle = deep && !run[i];
            if (idle) {
                idleTicks++; cur++;
                long t = T[i], t2 = t + 3_000_000L;
                Map<String, String> state = new HashMap<>();
                for (W w : exec) { if (w.s() > t2) break; if ((w.n() == 1 && w.s() >= t) || (w.n() > 1 && w.s() <= t && w.e() >= t)) state.put(w.thread(), w.sig()); }
                threadsSeen += state.size();
                for (String v : state.values()) sigCount.merge(v, 1L, Long::sum);
                var vmE = vmMap.subMap(t - 1_000_000L, t2); if (!vmE.isEmpty()) { vmTicks++; for (String v : vmE.values()) vmAt.merge(v, 1L, Long::sum); }
            } else if (cur > 0) {
                runs.add(cur); cur = 0;
                // what non-executor threads run in the tick that ends the gap (likely submitters)
                int lo = Drain.up(AR, T[i] - 1), hi = Drain.up(AR, T[i] + 3_000_000L);
                for (int k = lo; k < hi; k++) resumer.merge(anyRunWho.get(AR[k]), 1L, Long::sum);
            }
        }
        Collections.sort(runs);
        System.out.printf("== %s: %d store-backlog ticks with no executor thread RUNNABLE; executor threads with a known state per such tick: %.1f of 64%n", Path.of(a[0]).getFileName(), idleTicks, (double) threadsSeen / idleTicks);
        System.out.println("  executor thread states in those ticks (thread-ticks):");
        long tot = sigCount.values().stream().mapToLong(x -> x).sum();
        sigCount.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(6).forEach(x -> System.out.printf("    %5.1f%%  %s%n", 100.0 * x.getValue() / tot, x.getKey()));
        System.out.printf("  VM/GC threads running in %.1f%% of those ticks%n", 100.0 * vmTicks / idleTicks);
        vmAt.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(5).forEach(x -> System.out.printf("    %5d  %s%n", x.getValue(), x.getKey().length() > 200 ? x.getKey().substring(0, 200) : x.getKey()));
        System.out.printf("  gap length in ticks (~10ms): n=%d p50=%d p90=%d p99=%d max=%d; share of idle ticks in gaps >= 3 ticks: %.0f%%%n", runs.size(), runs.get(runs.size() / 2), runs.get(runs.size() * 9 / 10), runs.get(runs.size() * 99 / 100), runs.get(runs.size() - 1),
            100.0 * runs.stream().filter(r -> r >= 3).mapToLong(r -> r).sum() / runs.stream().mapToLong(r -> r).sum());
        System.out.println("  non-executor threads running in the tick that ends a gap:");
        long rt = resumer.values().stream().mapToLong(x -> x).sum();
        resumer.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(6).forEach(x -> System.out.printf("    %5.1f%%  %s%n", 100.0 * x.getValue() / rt, x.getKey()));
    }
}
