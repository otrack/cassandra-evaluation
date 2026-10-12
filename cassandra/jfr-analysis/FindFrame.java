import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// FindFrame <jfr> <frameSubstring> [from to]: list timestamps/threads of samples whose stack contains the frame
public class FindFrame {
    public static void main(String[] a) throws Exception {
        LocalTime from = a.length > 2 ? LocalTime.parse(a[2]) : LocalTime.MIN, to = a.length > 3 ? LocalTime.parse(a[3]) : LocalTime.MAX;
        List<String> out = new ArrayList<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); String ev = e.getEventType().getName();
                if (!ev.equals("profiler.WallClockSample") && !ev.equals("jdk.ExecutionSample")) continue;
                LocalTime t = LocalTime.ofInstant(e.getStartTime(), ZoneOffset.UTC);
                if (t.isBefore(from) || t.isAfter(to)) continue;
                RecordedStackTrace st = e.getStackTrace(); if (st == null) continue;
                boolean hit = false; for (RecordedFrame f : st.getFrames()) if ((f.getMethod().getType().getName() + "." + f.getMethod().getName()).contains(a[1])) { hit = true; break; }
                if (!hit) continue;
                RecordedThread th = e.getThread("sampledThread");
                String top = st.getFrames().isEmpty() ? "" : st.getFrames().get(0).getMethod().getType().getName().replaceAll(".*\\.","") + "." + st.getFrames().get(0).getMethod().getName();
                out.add(t + " " + (ev.startsWith("jdk") ? "CPU " : e.getString("state").replace("STATE_","") + " n=" + e.getLong("samples")) + " " + (th == null ? "?" : th.getJavaName()) + " " + top);
            }
        }
        Collections.sort(out); out.forEach(System.out::println);
    }
}
