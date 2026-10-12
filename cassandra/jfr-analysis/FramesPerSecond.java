import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.regex.*;
// FramesPerSecond <jfr> <regex> : per second, count of CPU samples whose stack contains a class matching regex (grouped by the matched simple class)
public class FramesPerSecond {
    public static void main(String[] a) throws Exception {
        Pattern p = Pattern.compile(a[1]);
        TreeMap<Long, Map<String,Integer>> m = new TreeMap<>(); Map<String,Integer> tot = new TreeMap<>();
        String ev = a.length > 2 ? a[2] : "jdk.ExecutionSample";
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals(ev)) continue;
                RecordedStackTrace st = e.getStackTrace(); if (st == null) continue;
                Set<String> hits = new TreeSet<>();
                for (RecordedFrame fr : st.getFrames()) { String n = fr.getMethod().getType().getName(); Matcher mm = p.matcher(n); if (mm.find()) hits.add(n.replaceAll("^.*\\.", "").replaceAll("\\$.*","")); }
                if (hits.isEmpty()) continue;
                String k = String.join("+", hits);
                m.computeIfAbsent(e.getStartTime().getEpochSecond(), x -> new TreeMap<>()).merge(k, 1, Integer::sum);
                tot.merge(k, 1, Integer::sum);
            }
        }
        System.out.println("== " + Path.of(a[0]).getFileName() + " " + tot);
        for (var en : m.entrySet()) System.out.println("  " + LocalTime.ofSecondOfDay(en.getKey() % 86400) + " " + en.getValue());
    }
}
