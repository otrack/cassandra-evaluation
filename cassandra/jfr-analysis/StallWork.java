import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// StallWork <stallsFile> <node> <jfr>: executor CPU samples inside that node's stall windows, by kind of work
public class StallWork {
    public static void main(String[] a) throws Exception {
        List<long[]> w = new ArrayList<>();
        for (String l : Files.readAllLines(Path.of(a[0]))) {
            String[] p = l.split("\\s+"); if (!p[0].equals(a[1])) continue;
            long base = Instant.parse(p[1]).toEpochMilli();
            w.add(new long[]{base + (long)(Double.parseDouble(p[2]) * 1000) - 5, base + (long)(Double.parseDouble(p[3]) * 1000)});
        }
        Map<String,Integer> m = new TreeMap<>(); Map<String,Integer> outside = new TreeMap<>(); long inMs = 0; for (long[] x : w) inMs += x[1]-x[0];
        try (RecordingFile rf = new RecordingFile(Path.of(a[2]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals("jdk.ExecutionSample")) continue;
                RecordedThread th = e.getThread("sampledThread");
                if (th == null || th.getJavaName() == null || !th.getJavaName().startsWith("AccordExecutor")) continue;
                long t = e.getStartTime().toEpochMilli(); boolean in = false;
                for (long[] x : w) if (t >= x[0] && t < x[1]) { in = true; break; }
                String k = "other";
                for (RecordedFrame f : e.getStackTrace().getFrames()) {
                    String n = f.getMethod().getType().getName().replaceAll(".*\\.","") + "." + f.getMethod().getName();
                    if (n.startsWith("IOTaskSave")) { k = "IOTaskSave"; break; }
                    if (n.startsWith("IOTaskLoad")) { k = "IOTaskLoad"; break; }
                    if (n.equals("AccordCacheEntry.save") || n.startsWith("CommandsForKeyAccessor.makeUpdate")) { k = "CFK save (inline)"; break; }
                    if (n.equals("AccordExecutor.unregisterExclusive")) { k = "unregisterExclusive"; break; }
                    if (n.equals("SafeTask.runMayThrow")) { k = "store task"; break; }
                    if (n.startsWith("PlainRunnable") || n.startsWith("PlainChain")) { k = "plain"; break; }
                    if (n.equals("AccordExecutor.lock") || n.equals("ReentrantLock.lock")) { k = "lock"; break; }
                }
                (in ? m : outside).merge(k, 1, Integer::sum);
            }
        }
        int ti = m.values().stream().mapToInt(i->i).sum(), to = outside.values().stream().mapToInt(i->i).sum();
        System.out.printf("%s: %d stall windows, %d ms; executor CPU samples in stalls %d (%.1f/ms), outside %d%n", a[1], w.size(), inMs, ti, ti/(double)inMs, to);
        for (String k : new TreeSet<>(outside.keySet()) {{ addAll(m.keySet()); }})
            System.out.printf("   %-22s in=%5.1f%%  out=%5.1f%%%n", k, 100.0*m.getOrDefault(k,0)/Math.max(1,ti), 100.0*outside.getOrDefault(k,0)/Math.max(1,to));
    }
}
