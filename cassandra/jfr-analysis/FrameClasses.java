import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
// FrameClasses <jfr> <regex> [event]: total samples whose stack contains a class (simple name) matching regex, by matched class
public class FrameClasses {
    public static void main(String[] a) throws Exception {
        Pattern p = Pattern.compile(a[1]); String ev = a.length > 2 ? a[2] : "jdk.ExecutionSample";
        Map<String,Long> tot = new TreeMap<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals(ev)) continue;
                RecordedStackTrace st = e.getStackTrace(); if (st == null) continue;
                Set<String> hits = new TreeSet<>();
                for (RecordedFrame fr : st.getFrames()) { String n = fr.getMethod().getType().getName(); if (p.matcher(n).find()) hits.add(n.replaceAll("^.*\\.", "").replaceAll("\\$.*","")); }
                long w = e.hasField("samples") ? e.getLong("samples") : 1;
                for (String h : hits) tot.merge(h, w, Long::sum);
            }
        }
        System.out.println(Path.of(a[0]).getFileName().toString().replaceAll(".*_","") + " " + tot);
    }
}
