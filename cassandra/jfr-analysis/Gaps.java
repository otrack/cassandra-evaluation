import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// Gaps <file> : group RUNNABLE wall samples into sampler ticks; in deep-backlog ticks, classify whether a store task
// (SafeTask) is running and, if not, what else is; compare read activity in store-idle vs store-busy ticks.
public class Gaps {
    static long ns(java.time.Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    record Smp(long t, String thread, String cls, String detail) {}
    static String cls(List<String> n) {
        boolean safe = false, read = false, plain = false, io = false, loop = false, idle = false, lock = false;
        for (String x : n) {
            if (x.endsWith("TxnNamedRead$1.call") || x.contains("TxnNamedRead")) read = true;
            if (x.contains("SafeTask.")) safe = true;
            if (x.contains(".PlainChain") || x.contains(".PlainRunnable")) plain = true;
            if (x.contains(".IOTask")) io = true;
            if (x.endsWith("awaitExclusive")) idle = true;
            if (x.endsWith("ReentrantLock$Sync.lock") || x.endsWith("AccordExecutor.lock")) lock = true;
            if (x.matches(".*AbstractLockLoop\\$\\d+\\.run")) loop = true;
        }
        if (safe) return "store";
        if (read) return "read";
        if (plain) return "plain(other)";
        if (io) return "io";
        if (idle) return "park/unpark";
        if (lock) return "lock";
        return loop ? "loop(poll/handoff)" : "outside loop";
    }
    public static void main(String[] a) throws Exception {
        List<long[]> q = new ArrayList<>(); List<Smp> exec = new ArrayList<>(); List<Long> allTimes = new ArrayList<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); String ty = e.getEventType().getName();
                if (ty.equals("profiler.Span") && Tags.isQueued(e.getString("tag"))) { q.add(new long[]{ns(e.getStartTime()), ns(e.getEndTime())}); continue; }
                if (!ty.equals("profiler.WallClockSample") || e.getLong("samples") != 1) continue;
                long t = ns(e.getStartTime()); allTimes.add(t);
                if (!e.getString("state").contains("RUNNABLE") || e.getStackTrace() == null) continue;
                RecordedThread th = e.getThread("sampledThread"); if (th == null || th.getJavaName() == null || !th.getJavaName().startsWith("AccordExecutor")) continue;
                List<String> n = new ArrayList<>(); for (RecordedFrame f : e.getStackTrace().getFrames()) n.add(f.getMethod().getType().getName() + "." + f.getMethod().getName());
                String c = cls(n), d = "";
                if (c.startsWith("loop") || c.equals("outside loop")) {
                    int lp = -1; for (int k = 0; k < n.size(); k++) if (n.get(k).matches(".*AbstractLockLoop\\$\\d+\\.run")) { lp = k; break; }
                    String callee = lp > 0 ? n.get(lp - 1).replaceAll(".*\\.([^.]+\\.[^.]+)$", "$1") : "-";
                    String inner = "-"; for (String x : n) if (x.startsWith("org.apache.cassandra.") || x.startsWith("accord.")) { inner = x.replaceAll(".*\\.([^.]+\\.[^.]+)$", "$1"); break; }
                    d = c + ": " + callee + " / " + inner;
                }
                exec.add(new Smp(t, th.getJavaName(), c, d));
            }
        }
        // ticks: cluster all un-batched wall sample timestamps; a new tick starts after a >3ms quiet period
        Collections.sort(allTimes); List<long[]> ticks = new ArrayList<>();
        long start = allTimes.get(0), last = start;
        for (long t : allTimes) { if (t - last > 3_000_000L) { ticks.add(new long[]{start, last}); start = t; } last = t; }
        ticks.add(new long[]{start, last});
        long[] tickStart = ticks.stream().mapToLong(x -> x[0]).toArray();
        List<Long> spacing = new ArrayList<>(); for (int i = 1; i < tickStart.length; i++) spacing.add((tickStart[i] - tickStart[i - 1]) / 1000); Collections.sort(spacing);
        long[] st = q.stream().mapToLong(x -> x[0]).sorted().toArray(), en = q.stream().mapToLong(x -> x[1]).sorted().toArray();
        Map<Integer, Set<String>> byTick = new HashMap<>(); Map<Integer, List<String>> det = new HashMap<>();
        for (Smp s : exec) { int i = Arrays.binarySearch(tickStart, s.t()); if (i < 0) i = -i - 2; if (i >= 0) { byTick.computeIfAbsent(i, k -> new HashSet<>()).add(s.cls()); if (!s.detail().isEmpty()) det.computeIfAbsent(i, k -> new ArrayList<>()).add(s.detail()); } }
        long deep = 0, busy = 0; Map<String, Long> loopDet = new HashMap<>(); Map<String, Long> causes = new TreeMap<>(); long loopN = 0; Map<String, Long> idleWhat = new TreeMap<>(); long busyRead = 0, idleRead = 0, busyPlain = 0, idlePlain = 0;
        for (int i = 0; i < tickStart.length; i++) {
            if (Drain.up(st, tickStart[i]) - Drain.up(en, tickStart[i]) < 50) continue;
            deep++; Set<String> c = byTick.getOrDefault(i, Set.of());
            boolean r = c.contains("read"), p = c.contains("plain(other)") || r;
            if (c.contains("store")) { busy++; if (r) busyRead++; if (p) busyPlain++; }
            else {
                if (r) idleRead++; if (p) idlePlain++;
                for (String d : det.getOrDefault(i, List.of())) { loopDet.merge(d, 1L, Long::sum); loopN++; }
                List<String> ds = det.getOrDefault(i, List.of());
                String cause;
                if (ds.stream().anyMatch(d -> d.startsWith("outside loop") && d.matches(".*(Serializ|serializ).*")) || ds.stream().anyMatch(d -> d.contains("completeExclusiveNoExcept") && d.matches(".*(Serializ|serializ|Journal).*"))) cause = "store work, stack misattributed (serialization)";
                else if (r) cause = "read running (TxnNamedRead)";
                else if (p || c.contains("io")) cause = "other off-store task running";
                else if (c.contains("lock")) cause = "executor lock wait";
                else if (ds.stream().anyMatch(d -> d.contains("DebugExecution"))) cause = "DebugExecution timing (now disabled)";
                else if (c.contains("loop(poll/handoff)") || c.contains("park/unpark")) cause = "handoff: poll / unlock / park / unpark";
                else if (c.isEmpty()) cause = "nothing running on any executor thread";
                else cause = "other (" + String.join("+", new TreeSet<>(c)) + ")";
                causes.merge(cause, 1L, Long::sum);
                String what = c.isEmpty() ? "nothing running on any executor thread" : String.join("+", new TreeSet<>(c));
                idleWhat.merge(what, 1L, Long::sum);
            }
        }
        long idle = deep - busy;
        System.out.printf("== %s: tick spacing p10/p50/p90 = %d/%d/%d us; deep-backlog ticks %d%n", Path.of(a[0]).getFileName(), spacing.get(spacing.size() / 10), spacing.get(spacing.size() / 2), spacing.get(spacing.size() * 9 / 10), deep);
        System.out.printf("  store task running in %.1f%% of ticks; store idle in %.1f%%%n", 100.0 * busy / deep, 100.0 * idle / deep);
        System.out.printf("  a read running:            in store-busy ticks %.1f%%   in store-idle ticks %.1f%%%n", 100.0 * busyRead / busy, 100.0 * idleRead / idle);
        System.out.printf("  any off-store plain task:  in store-busy ticks %.1f%%   in store-idle ticks %.1f%%%n", 100.0 * busyPlain / busy, 100.0 * idlePlain / idle);
        System.out.println("  what the executor threads are doing in store-idle ticks (share of idle ticks):");
        idleWhat.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(9).forEach(x -> System.out.printf("    %5.1f%%  %s%n", 100.0 * x.getValue() / idle, x.getKey()));
        System.out.println("  store-idle ticks by primary cause, as % of the store's timeline:");
        final long DD = deep; causes.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).forEach(x -> System.out.printf("    %5.1f%%  %s%n", 100.0 * x.getValue() / DD, x.getKey()));
        if (true) return;
        final long LN = loopN;
        System.out.println("  loop / outside-loop samples in store-idle ticks, by loop callee / innermost cassandra-or-accord frame:");
        loopDet.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(12).forEach(x -> System.out.printf("    %5.1f%%  %s%n", 100.0 * x.getValue() / LN, x.getKey()));
    }
}
