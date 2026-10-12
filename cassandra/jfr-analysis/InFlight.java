import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// InFlight <jfr> : per second, max and mean number of SharedPool workers parked in AccordResult.awaitAndGet (sampled at 100ms grid), and number of distinct workers seen
public class InFlight {
    public static void main(String[] a) throws Exception {
        Map<String, List<long[]>> m = new HashMap<>(); // thread -> [start,end,in]
        Map<String, List<long[]>> any = new HashMap<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals("profiler.WallClockSample")) continue;
                RecordedThread th = e.getThread("sampledThread");
                if (th == null || th.getJavaName() == null || !th.getJavaName().startsWith("SharedPool-Worker")) continue;
                boolean in = false; RecordedStackTrace st = e.getStackTrace();
                if (st != null) for (RecordedFrame f : st.getFrames()) if ((f.getMethod().getType().getName() + "." + f.getMethod().getName()).endsWith("AccordResult.awaitAndGet")) { in = true; break; }
                long t = e.getStartTime().toEpochMilli();
                m.computeIfAbsent(th.getJavaName(), k -> new ArrayList<>()).add(new long[]{t, t + e.getDuration("timeSpan").toMillis(), in ? 1 : 0});
            }
        }
        long lo = Long.MAX_VALUE, hi = 0;
        for (var l : m.values()) for (long[] x : l) { lo = Math.min(lo, x[0]); hi = Math.max(hi, x[1]); }
        lo = lo / 1000 * 1000;
        int nb = (int)((hi - lo) / 50) + 1; int[] cnt = new int[nb];
        for (var l : m.values()) { l.sort(Comparator.comparingLong(x -> x[0])); for (int i = 0; i < l.size(); i++) { long[] x = l.get(i); if (x[2] == 0) continue; long end = Math.max(x[1], x[0] + 10); for (long t = (x[0]+49)/50*50; t <= end; t += 50) cnt[(int)((t - lo)/50)]++; } }
        System.out.println(Path.of(a[0]).getFileName() + " workers=" + m.size());
        for (int s = 0; s < nb / 20; s++) { int mx = 0; double sum = 0; for (int j = 0; j < 20; j++) { mx = Math.max(mx, cnt[s*20+j]); sum += cnt[s*20+j]; } if (mx > 0) System.out.printf("  %s max=%d mean=%.1f%n", LocalTime.ofInstant(Instant.ofEpochMilli(lo + s*1000L), ZoneOffset.UTC), mx, sum/20); }
    }
}
