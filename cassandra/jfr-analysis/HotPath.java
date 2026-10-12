import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// HotPath <file> : during deep-backlog 10ms buckets (>=50 AccordTaskQueued open), attribute executor-thread time
// (wall RUNNABLE samples = 10ms each; CPU = ctimer 1ms each) to task kind / phase / component.
// Store tasks (SafeTask) run exclusively on their command store, so their wall time is the hot store's serial timeline.
public class HotPath {
    static long ns(java.time.Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    static final String[][] RULES = {
        {"cache size accounting (updateSize)", "AccordCacheEntry.updateSize", "estimateHeapSize", "AccordObjectSizes."},
        {"journal (serialize + write)", ".journal.", "AccordJournal", "CommandChangeWriter"},
        {"deps calc (CommandsForKey.visit)", "DepsCalculator", "CommandsForKey.visit", "SaferCommandStore.visit"},
        {"CommandsForKey update (insertOrUpdate...)", "accord.local.cfk."},
        {"read (Txn.execute)", "Txn.execute", "SinglePartitionReadCommand", "TxnRead"},
        {"write apply", "TxnWrite", "Mutation.apply", "Keyspace.apply", "Memtable.put"},
        {"messaging send", "org.apache.cassandra.net."},
        {"debug instrumentation", ".accord.debug."},
        {"cache acquire/release (other)", "AccordCache", "AccordCacheEntry"},
    };
    public static void main(String[] a) throws Exception {
        long B = 10_000_000L; List<long[]> q = new ArrayList<>(); List<RecordedEvent> samples = new ArrayList<>(); long t0 = Long.MAX_VALUE, t1 = 0;
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); long s = ns(e.getStartTime()); t0 = Math.min(t0, s); t1 = Math.max(t1, s);
                String ty = e.getEventType().getName();
                if (ty.equals("profiler.Span") && Tags.isQueued(e.getString("tag"))) q.add(new long[]{s, ns(e.getEndTime())});
                else if (ty.equals("jdk.ExecutionSample") || (ty.equals("profiler.WallClockSample") && e.getString("state").contains("RUNNABLE"))) {
                    RecordedThread t = e.getThread("sampledThread");
                    if (t != null && t.getJavaName() != null && t.getJavaName().startsWith("AccordExecutor")) samples.add(e);
                }
            }
        }
        long[] st = q.stream().mapToLong(x -> x[0]).sorted().toArray(), en = q.stream().mapToLong(x -> x[1]).sorted().toArray();
        Set<Long> deep = new HashSet<>();
        for (long b = t0 / B; b <= t1 / B; b++) { long mid = b * B + B / 2; if (Drain.up(st, mid) - Drain.up(en, mid) >= 50) deep.add(b); }
        double deepSec = deep.size() * 0.01;
        Map<String, double[]> m = new TreeMap<>(); // key -> {wallSec, cpuSec}
        Map<String, double[]> reads = new TreeMap<>();
        for (RecordedEvent e : samples) {
            if (!deep.contains(ns(e.getStartTime()) / B) || e.getStackTrace() == null) continue;
            boolean wall = e.getEventType().getName().equals("profiler.WallClockSample");
            List<RecordedFrame> fs = e.getStackTrace().getFrames();
            List<String> names = new ArrayList<>(); for (RecordedFrame f : fs) names.add(f.getMethod().getType().getName() + "." + f.getMethod().getName());
            int loop = -1; for (int i = 0; i < names.size(); i++) if (names.get(i).matches(".*AbstractLockLoop\\$\\d+\\.run")) { loop = i; break; }
            String phase = loop <= 0 ? "outside loop" : names.get(loop - 1).replaceAll(".*\\.([^.]+\\.[^.]+)$", "$1");
            String kind;
            if (phase.equals("Task.runNoExcept") || phase.equals("Task.completeExclusiveNoExcept") || phase.equals("Task.prepareExclusiveNoExcept")) {
                kind = "other task";
                for (int i = loop - 1; i >= 0; i--) { String n = names.get(i);
                    if (n.contains("SafeTask.")) { kind = "STORE task (SafeTask)"; break; }
                    if (n.contains(".PlainChain") || n.contains(".PlainRunnable")) { kind = "plain task (off-store)"; break; }
                    if (n.contains(".IOTask")) { kind = "IO task"; break; } }
                phase = phase.replace("Task.", "").replace("NoExcept", "").replace("Exclusive", "");
            } else { kind = "executor loop"; }
            String comp = "other";
            if (!kind.equals("executor loop")) {
                outer: for (String[] r : RULES) for (int k = 1; k < r.length; k++) for (int i = 0; i < loop; i++) if (names.get(i).contains(r[k])) { comp = r[0]; break outer; }
            } else comp = phase;
            double[] v = m.computeIfAbsent(kind + " | " + (kind.equals("executor loop") ? "" : phase + " | ") + comp, x -> new double[2]);
            if (wall) v[0] += 0.010; else v[1] += 0.001;
            // read path detail: memtable-only vs touching sstables
            if (comp.startsWith("read")) {
                boolean sst = false, mem = false;
                for (String n : names) { if (n.matches(".*(SSTableReader|BigTableReader|BtiTableReader|RandomAccessReader|ChunkCache|BloomFilter|SSTableIterator|RowIndexEntry|PartitionIndex).*")) sst = true; if (n.contains("Memtable")) mem = true; }
                String where = sst ? "touches SSTables" : mem ? "memtable only" : "neither visible (read setup/merge)";
                double[] r = reads.computeIfAbsent(where, x -> new double[2]); if (wall) r[0] += 0.010; else r[1] += 0.001;
            }
        }
        System.out.printf("== %s: deep backlog %.1fs of %.0fs; times below are thread-seconds per second of backlog (1.00 = one thread busy throughout)%n", Path.of(a[0]).getFileName(), deepSec, (t1 - t0) / 1e9);
        System.out.printf("  %-78s %7s %7s%n", "kind | phase | component", "wall", "cpu*");
        System.out.println("  (*cpu counts each ctimer sample as 1ms; on HZ=250 kernels a sample is ~4ms, so multiply by ~4. wall is reliable.)");
        double storeWall = 0, storeCpu = 0;
        List<Map.Entry<String, double[]>> l = new ArrayList<>(m.entrySet()); l.sort((x, y) -> Double.compare(y.getValue()[0], x.getValue()[0]));
        for (var x : l) { if (x.getValue()[0] / deepSec < 0.004 && x.getValue()[1] / deepSec < 0.004) continue;
            System.out.printf("  %-78s %7.3f %7.3f%n", x.getKey(), x.getValue()[0] / deepSec, x.getValue()[1] / deepSec);
            if (x.getKey().startsWith("STORE")) { storeWall += x.getValue()[0]; storeCpu += x.getValue()[1]; } }
        System.out.printf("  STORE tasks total: wall %.3f, cpu %.3f  -> store timeline with no store task running: %.0f%%%n", storeWall / deepSec, storeCpu / deepSec, 100 * (1 - storeWall / deepSec));
        reads.forEach((k, v) -> System.out.printf("  read path: %-36s wall %.3f cpu %.3f%n", k, v[0] / deepSec, v[1] / deepSec));
    }
}
