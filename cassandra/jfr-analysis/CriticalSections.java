import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// CriticalSections <jfr> <minMs>: critical sections >= minMs, and CPU samples of the holder thread during them (top frames below AbstractLockLoop)
public class CriticalSections {
    public static void main(String[] a) throws Exception {
        long min = Long.parseLong(a[1]) * 1_000_000L;
        List<RecordedEvent> cs = new ArrayList<>(); List<RecordedEvent> smp = new ArrayList<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); String ev = e.getEventType().getName();
                if (ev.equals("profiler.Span") && e.getString("tag").equals("AccordExecutorCriticalSection") && e.getDuration().toNanos() >= min) cs.add(e);
                else if (ev.equals("jdk.ExecutionSample") || ev.equals("profiler.WallClockSample")) { RecordedThread t = e.getThread("sampledThread"); if (t != null && t.getJavaName() != null && t.getJavaName().startsWith("AccordExecutor")) smp.add(e); }
            }
        }
        Map<String,Integer> agg = new HashMap<>(); int n = 0;
        for (RecordedEvent c : cs) {
            String th = c.getThread() == null ? "?" : c.getThread().getJavaName();
            Instant s = c.getStartTime(), e = c.getEndTime();
            for (RecordedEvent x : smp) {
                if (!th.equals(x.getThread("sampledThread").getJavaName())) continue;
                if (x.getStartTime().isBefore(s) || x.getStartTime().isAfter(e)) continue;
                n++;
                List<RecordedFrame> fr = x.getStackTrace().getFrames(); StringBuilder sb = new StringBuilder();
                int idx = -1; for (int i = 0; i < fr.size(); i++) { String m = fr.get(i).getMethod().getType().getName(); if (m.endsWith("AbstractLockLoop$2")) { idx = i; break; } }
                int from = Math.max(0, idx - Integer.getInteger("d", 6)); if (idx < 0) { idx = Math.min(8, fr.size()); from = 0; }
                for (int i = idx - 1; i >= from; i--) sb.append(fr.get(i).getMethod().getType().getName().replaceAll(".*\\.","")).append('.').append(fr.get(i).getMethod().getName()).append(" > ");
                agg.merge(sb.toString(), 1, Integer::sum);
            }
        }
        System.out.println(Path.of(a[0]).getFileName() + ": critical sections >= " + a[1] + "ms: " + cs.size() + ", holder samples " + n);
        agg.entrySet().stream().sorted((x,y)->y.getValue()-x.getValue()).limit(8).forEach(x -> System.out.println("  " + x.getValue() + "  " + x.getKey()));
    }
}
