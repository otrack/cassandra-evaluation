// ThreadSamples <jfr> <threadName> <fromHH:MM:SS.sss> <toHH:MM:SS.sss> : every wall/CPU sample of one thread, with state,
// batch size (samples=, timeSpan) and the top frames; shows how batched idle samples bound each park.
import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
public class ThreadSamples {
    public static void main(String[] a) throws Exception {
        String thr = a[1]; LocalTime from = LocalTime.parse(a[2]), to = LocalTime.parse(a[3]);
        List<String> out = new ArrayList<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                String ev = e.getEventType().getName();
                if (!ev.equals("profiler.WallClockSample") && !ev.equals("jdk.ExecutionSample")) continue;
                RecordedThread th = e.getThread("sampledThread");
                if (th == null || !thr.equals(th.getJavaName())) continue;
                LocalTime t = LocalTime.ofInstant(e.getStartTime(), ZoneOffset.UTC);
                if (t.isBefore(from) || t.isAfter(to)) continue;
                String top = ""; RecordedStackTrace st = e.getStackTrace();
                if (st != null) { int i = 0; for (RecordedFrame f : st.getFrames()) { String n = f.getMethod().getType().getName().replaceAll(".*\\.","") + "." + f.getMethod().getName(); if (n.startsWith("lib") || n.startsWith(".") || n.startsWith("so.")) continue; if (i++ < 3) top += n + " < "; if (n.contains("AccordService.coordinate") || n.contains("SEPWorker.run")) { top += "... " + n; break; } } }
                out.add(t + " " + (ev.startsWith("jdk") ? "CPU" : e.getString("state").replace("STATE_","") + " n=" + e.getLong("samples") + " span=" + e.getDuration("timeSpan").toMillis()) + "  " + top);
            }
        }
        Collections.sort(out); out.forEach(System.out::println);
    }
}
