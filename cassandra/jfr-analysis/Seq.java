import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// Seq <file> <thread> <fromSec> <count> : spans on one thread, ordered by end, times relative to recording start (ms, us precision)
public class Seq {
    public static void main(String[] a) throws Exception {
        record S(long s, long e, String tag) {}
        List<S> l = new ArrayList<>(); Instant t0 = null; Set<String> threads = new TreeSet<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (t0 == null || e.getStartTime().isBefore(t0)) t0 = e.getStartTime();
                if (!e.getEventType().getName().equals("profiler.Span")) continue;
                String tn = e.getThread().getJavaName(); threads.add(tn);
                if (!a[1].equals(tn)) continue;
                l.add(new S(e.getStartTime().getEpochSecond()*1_000_000_000L + e.getStartTime().getNano(), e.getEndTime().getEpochSecond()*1_000_000_000L + e.getEndTime().getNano(), e.getString("tag")));
            }
        }
        long base = t0.getEpochSecond()*1_000_000_000L + t0.getNano();
        System.out.println("span-emitting threads: " + threads.stream().filter(x -> x.startsWith("AccordExecutor")).count() + " AccordExecutor threads");
        l.sort(Comparator.comparingLong(S::e));
        double from = Double.parseDouble(a[2]); int n = Integer.parseInt(a[3]);
        l.stream().filter(s -> (s.e() - base) / 1e9 >= from).limit(n).forEach(s ->
            System.out.printf("  start %10.3fms  end %10.3fms  dur %9.3fms  %s%n", (s.s()-base)/1e6, (s.e()-base)/1e6, (s.e()-s.s())/1e6, s.tag()));
    }
}
