// StoreSpanRate <jfr>... : per second, count of store-task run spans (either tag format) per file; a crude activity measure.
import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
public class StoreSpanRate {
    public static void main(String[] a) throws Exception {
        TreeMap<Long, long[]> m = new TreeMap<>();
        for (int i = 0; i < a.length; i++) {
            try (RecordingFile rf = new RecordingFile(Path.of(a[i]))) {
                while (rf.hasMoreEvents()) {
                    RecordedEvent e = rf.readEvent();
                    if (!e.getEventType().getName().equals("profiler.Span")) continue;
                    String tag = e.getString("tag");
                    if (!Tags.isRun(tag) || !Tags.isStoreTask(tag)) continue; // store task runs
                    m.computeIfAbsent(e.getStartTime().getEpochSecond(), k -> new long[a.length])[i]++;
                }
            }
        }
        for (var en : m.entrySet()) { System.out.print(LocalTime.ofSecondOfDay(en.getKey()%86400)); for (long v : en.getValue()) System.out.printf(" %5d", v); System.out.println(); }
    }
}
