import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// WindowAbs <jfr> <fromHH:MM:SS.sss> <toHH:MM:SS.sss> <threadRegex> <depth> <top> [event] [frameFilterRegex]
public class WindowAbs {
    public static void main(String[] a) throws Exception {
        LocalTime from = LocalTime.parse(a[1]), to = LocalTime.parse(a[2]);
        String re = a[3]; int depth = Integer.parseInt(a[4]); int top = Integer.parseInt(a[5]);
        String ev = a.length > 6 ? a[6] : "jdk.ExecutionSample"; String ff = a.length > 7 ? a[7] : null;
        Map<String,Long> m = new HashMap<>(); Map<String,Long> names = new HashMap<>(); long tot=0;
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals(ev)) continue;
                LocalTime t = LocalTime.ofInstant(e.getStartTime(), ZoneOffset.UTC);
                if (t.isBefore(from) || !t.isBefore(to)) continue;
                RecordedThread th = e.hasField("sampledThread") ? e.getThread("sampledThread") : e.getThread();
                String n = th == null ? "?" : (th.getJavaName() != null ? th.getJavaName() : "[tid]");
                if (!n.matches(re)) continue;
                long w = e.hasField("samples") ? e.getLong("samples") : 1;
                String state = e.hasField("state") ? e.getString("state") : "";
                tot+=w; names.merge(n.replaceAll("\\d+","N"), w, Long::sum);
                StringBuilder sb = new StringBuilder(" " + state);
                RecordedStackTrace st = e.getStackTrace();
                if (st != null) { int i=0; for (RecordedFrame fr : st.getFrames()) { String mn = fr.getMethod().getType().getName().replaceAll("^.*\\.(?=[^.]+\\.[^.]+$)","") + "." + fr.getMethod().getName(); if (ff != null && !mn.matches(ff) && i==0) continue; if (i++>=depth) break; sb.append("\n      ").append(mn).append(":").append(fr.getLineNumber()); } }
                m.merge(sb.toString(), w, Long::sum);
            }
        }
        System.out.println("total " + tot);
        names.entrySet().stream().sorted((x,y)->Long.compare(y.getValue(),x.getValue())).limit(12).forEach(x->System.out.println("  thread " + x));
        m.entrySet().stream().sorted((x,y)->Long.compare(y.getValue(),x.getValue())).limit(top).forEach(x->System.out.println("  " + x.getValue() + x.getKey()));
    }
}
