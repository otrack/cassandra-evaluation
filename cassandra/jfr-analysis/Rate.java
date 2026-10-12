import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// Rate <file> : how often the wall sampler visits each thread (gap between consecutive wall samples of the same thread),
// and how many distinct threads the JVM has; batched events count as one visit at their end
public class Rate {
    static long ns(java.time.Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    public static void main(String[] a) throws Exception {
        Map<String, List<long[]>> m = new HashMap<>(); Set<String> all = new HashSet<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals("profiler.WallClockSample")) continue;
                RecordedThread t = e.getThread("sampledThread"); String tn = t == null ? "?" : t.getJavaName() != null ? t.getJavaName() : t.getOSName();
                all.add(tn);
                if (!tn.startsWith("AccordExecutor")) continue;
                long s = ns(e.getStartTime());
                m.computeIfAbsent(tn, k -> new ArrayList<>()).add(new long[]{s, e.getLong("samples"), e.getString("state").contains("RUNNABLE") ? 1 : 0, e.getDuration("timeSpan").toNanos()});
            }
        }
        List<Long> gapsRR = new ArrayList<>(); long batchedSamples = 0, batchedEvents = 0;
        for (List<long[]> l : m.values()) {
            l.sort(Comparator.comparingLong(x -> x[0]));
            for (int i = 1; i < l.size(); i++) if (l.get(i)[2] == 1 && l.get(i - 1)[2] == 1) gapsRR.add((l.get(i)[0] - l.get(i - 1)[0]) / 1000);
            for (long[] x : l) if (x[1] > 1) { batchedEvents++; batchedSamples += x[1]; }
        }
        Collections.sort(gapsRR);
        System.out.printf("%s: %d distinct sampled threads; executor RUNNABLE->RUNNABLE gap p10/p50/p90 = %d/%d/%d us; batched idle events %d covering %d samples%n",
            Path.of(a[0]).getFileName(), all.size(), gapsRR.get(gapsRR.size() / 10), gapsRR.get(gapsRR.size() / 2), gapsRR.get(gapsRR.size() * 9 / 10), batchedEvents, batchedSamples);
    }
}
