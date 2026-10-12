import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// Idle3 <file> : cross-check executor-idle backlog ticks against independent evidence of executor activity:
// ctimer CPU samples on executor threads within +-5ms, and executor spans (not AccordTaskQueued) covering the tick.
// Also: how many distinct threads the wall sampler visits per tick.
public class Idle3 {
    static long ns(java.time.Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    public static void main(String[] a) throws Exception {
        List<long[]> q = new ArrayList<>(); List<Long> times = new ArrayList<>(); List<Long> execRun = new ArrayList<>(); List<Long> execCpu = new ArrayList<>();
        List<long[]> execSpans = new ArrayList<>(); Map<Long, Set<String>> perTick = new HashMap<>(); List<long[]> wallAll = new ArrayList<>();
        Set<String> threads = new HashSet<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); String ty = e.getEventType().getName(); long s = ns(e.getStartTime());
                if (ty.equals("profiler.Span")) { String tag = e.getString("tag"); String tn = e.getThread().getJavaName();
                    if (Tags.isQueued(tag)) q.add(new long[]{s, ns(e.getEndTime())});
                    else if (tn != null && tn.startsWith("AccordExecutor")) execSpans.add(new long[]{s, ns(e.getEndTime())}); continue; }
                if (!ty.equals("jdk.ExecutionSample") && !ty.equals("profiler.WallClockSample")) continue;
                RecordedThread th = e.getThread("sampledThread"); String tn = th == null ? "?" : th.getJavaName() != null ? th.getJavaName() : th.getOSName();
                if (ty.equals("jdk.ExecutionSample")) { if (tn.startsWith("AccordExecutor")) execCpu.add(s); continue; }
                if (!ty.equals("profiler.WallClockSample")) continue;
                threads.add(tn);
                if (e.getLong("samples") == 1) { times.add(s); wallAll.add(new long[]{s, tn.hashCode()}); if (tn.startsWith("AccordExecutor") && e.getString("state").contains("RUNNABLE")) execRun.add(s); }
            }
        }
        Collections.sort(times); List<Long> tick = new ArrayList<>(); long last = -1;
        for (long t : times) { if (last < 0 || t - last > 3_000_000L) tick.add(t); last = t; }
        long[] T = tick.stream().mapToLong(x -> x).toArray();
        // distinct threads with an un-batched sample per tick
        int[] perTickCount = new int[T.length]; Map<Integer, Set<Long>> tt = new HashMap<>();
        for (long[] w : wallAll) { int i = Arrays.binarySearch(T, w[0]); if (i < 0) i = -i - 2; if (i >= 0) tt.computeIfAbsent(i, k -> new HashSet<>()).add(w[1]); }
        List<Integer> cnt = new ArrayList<>(); tt.values().forEach(x -> cnt.add(x.size())); Collections.sort(cnt);
        long[] st = q.stream().mapToLong(x -> x[0]).sorted().toArray(), en = q.stream().mapToLong(x -> x[1]).sorted().toArray();
        long[] ER = execRun.stream().mapToLong(x -> x).sorted().toArray(), EC = execCpu.stream().mapToLong(x -> x).sorted().toArray();
        long[] ss = execSpans.stream().mapToLong(x -> x[0]).sorted().toArray(), se = execSpans.stream().mapToLong(x -> x[1]).sorted().toArray();
        long idle = 0, withCpu = 0, withSpan = 0, either = 0, busy = 0, busyCpu = 0, idleTight = 0, busyTight = 0; List<Integer> idleSize = new ArrayList<>(), busySize = new ArrayList<>(); long idleFull = 0, idleFullNoCpu = 0;
        for (int i = 0; i < T.length; i++) {
            if (Drain.up(st, T[i]) - Drain.up(en, T[i]) < 50) continue;
            long t = T[i], next = i + 1 < T.length ? T[i + 1] : t + 10_000_000L;
            boolean run = Drain.up(ER, next - 1) - Drain.up(ER, t - 1) > 0;
            boolean cpu = Drain.up(EC, t + 5_000_000L) - Drain.up(EC, t - 5_000_000L) > 0;
            boolean span = Drain.up(ss, t) - Drain.up(se, t) > 0;
            boolean tight = Drain.up(EC, t + 1_000_000L) - Drain.up(EC, t - 1_000_000L) > 0;
            int size = tt.getOrDefault(i, Set.of()).size();
            if (run) { busy++; if (cpu) busyCpu++; if (tight) busyTight++; busySize.add(size); continue; }
            idleSize.add(size); if (tight) idleTight++;
            if (size >= 40) { idleFull++; if (!tight) idleFullNoCpu++; }
            idle++; if (cpu) withCpu++; if (span) withSpan++; if (cpu || span) either++;
        }
        System.out.printf("== %s: %d threads seen by the wall sampler; un-batched samples per tick p10/p50/p90 = %d/%d/%d%n", Path.of(a[0]).getFileName(), threads.size(), cnt.get(cnt.size() / 10), cnt.get(cnt.size() / 2), cnt.get(cnt.size() * 9 / 10));
        System.out.printf("  backlog ticks with no RUNNABLE executor wall sample: %d%n", idle);
        System.out.printf("    executor CPU sample within +-5ms: %.0f%% (vs %.0f%% of ticks where an executor thread is RUNNABLE)%n", 100.0 * withCpu / idle, 100.0 * busyCpu / busy);
        System.out.printf("    an executor span (run/critical section/IO) covers the tick: %.0f%%%n", 100.0 * withSpan / idle);
        Collections.sort(idleSize); Collections.sort(busySize);
        System.out.printf("    samples in the tick (is it a full sampling round?): idle ticks p10/p50/p90 = %d/%d/%d   busy ticks p10/p50/p90 = %d/%d/%d%n",
            idleSize.get(idleSize.size() / 10), idleSize.get(idleSize.size() / 2), idleSize.get(idleSize.size() * 9 / 10), busySize.get(busySize.size() / 10), busySize.get(busySize.size() / 2), busySize.get(busySize.size() * 9 / 10));
        System.out.printf("    executor CPU sample within +-1ms: idle ticks %.0f%%  busy ticks %.0f%%%n", 100.0 * idleTight / idle, 100.0 * busyTight / busy);
        System.out.printf("    idle ticks that are full rounds (>=40 samples): %d (%.2f%% of backlog ticks), of which with no executor CPU within +-1ms: %d%n", idleFull, 100.0 * idleFull / (idle + busy), idleFullNoCpu);
        System.out.printf("    either: %.0f%%  -> ticks with no evidence of any executor activity: %d (%.1f%% of backlog ticks)%n", 100.0 * either / idle, idle - either, 100.0 * (idle - either) / (idle + busy));
    }
}
