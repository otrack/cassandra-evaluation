import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// Stores <file>... : emitted run spans per command store (count, summed duration), plus share of total
public class Stores {
    public static void main(String[] a) throws Exception {
        for (String f : a) {
            Map<String, long[]> m = new HashMap<>(); long tot = 0, totN = 0;
            try (RecordingFile rf = new RecordingFile(Path.of(f))) {
                while (rf.hasMoreEvents()) {
                    RecordedEvent e = rf.readEvent();
                    if (!e.getEventType().getName().equals("profiler.Span")) continue;
                    String tag = e.getString("tag");
                    if (!Tags.isRun(tag) || !Tags.isStoreTask(tag)) continue;
                    String store = Tags.store(tag);
                    long d = e.getDuration().toNanos(); long[] v = m.computeIfAbsent(store, k -> new long[2]); v[0]++; v[1] += d; tot += d; totN++;
                }
            }
            final long T = tot, N = totN;
            System.out.println("== " + Path.of(f).getFileName() + "  (run spans " + N + ", summed " + T / 1_000_000 + "ms)");
            m.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue()[1], x.getValue()[1])).limit(6).forEach(x ->
                System.out.printf("  store %-3s  spans=%6d (%4.1f%%)  summed=%7.1fms (%4.1f%%)  mean=%6.0fus%n", x.getKey(), x.getValue()[0], 100.0 * x.getValue()[0] / N,
                    x.getValue()[1] / 1e6, 100.0 * x.getValue()[1] / T, x.getValue()[1] / 1e3 / x.getValue()[0]));
        }
    }
}
