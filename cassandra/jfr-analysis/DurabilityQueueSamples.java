import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// DurabilityQueueSamples <jfr>: samples (CPU+wall) whose stack contains DurabilityQueue frames; summarise DQ frame chain + whether ensureDurable/save is above it
public class DurabilityQueueSamples {
    public static void main(String[] a) throws Exception {
        Map<String,Integer> agg = new TreeMap<>(); List<String> withEnsure = new ArrayList<>(); int total = 0;
        Map<String,Integer> monitors = new TreeMap<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); String ev = e.getEventType().getName();
                if (ev.equals("jdk.JavaMonitorEnter")) {
                    RecordedClass c = e.getClass("monitorClass");
                    monitors.merge(c == null ? "?" : c.getName(), 1, Integer::sum);
                    if (c != null && c.getName().contains("Durability"))
                        withEnsure.add("MONITOR-ENTER " + e.getStartTime() + " " + e.getDuration().toMillis() + "ms thread=" + e.getThread().getJavaName());
                    continue;
                }
                if (!ev.equals("jdk.ExecutionSample") && !ev.equals("profiler.WallClockSample")) continue;
                RecordedStackTrace st = e.getStackTrace(); if (st == null) continue;
                List<RecordedFrame> fr = st.getFrames();
                StringBuilder dq = new StringBuilder(); int firstDq = -1;
                for (int i = 0; i < fr.size(); i++) {
                    RecordedFrame f = fr.get(i); String cls = f.getMethod().getType().getName();
                    if (cls.contains("DurabilityQueue")) { if (firstDq < 0) firstDq = i; dq.append(cls.replaceAll(".*\\.","")).append('.').append(f.getMethod().getName()).append(':').append(f.getLineNumber()).append(" < "); }
                }
                if (firstDq < 0) continue;
                total++;
                boolean ensure = false; StringBuilder above = new StringBuilder();
                for (int i = 0; i < firstDq; i++) { String m = fr.get(i).getMethod().getType().getName() + "." + fr.get(i).getMethod().getName(); if (m.contains("ensureDurable") || m.contains("maybeFlush") || m.contains("AccordCacheEntry.save") || m.contains("saveWhenReady")) ensure = true; }
                for (int i = Math.max(0, firstDq - 6); i < firstDq; i++) above.insert(0, fr.get(i).getMethod().getType().getName().replaceAll(".*\\.","") + "." + fr.get(i).getMethod().getName() + " > ");
                String state = ev.startsWith("jdk") ? "CPU" : e.getString("state").replace("STATE_","");
                long w = e.hasField("samples") ? e.getLong("samples") : 1;
                agg.merge(state + " | " + dq + " || " + above, (int) w, Integer::sum);
                if (ensure) withEnsure.add(e.getStartTime() + " " + state + " " + e.getThread("sampledThread").getJavaName() + " " + dq + " || " + above);
            }
        }
        System.out.println("== " + Path.of(a[0]).getFileName() + " samples with DurabilityQueue frames: " + total);
        agg.entrySet().stream().sorted((x,y)->y.getValue()-x.getValue()).limit(Integer.getInteger("top", 12)).forEach(x -> System.out.println("  " + x.getValue() + "  " + x.getKey()));
        System.out.println("  -- with ensureDurable/save above a DurabilityQueue frame, or DurabilityQueue monitor contention: " + withEnsure.size());
        withEnsure.stream().limit(20).forEach(s -> System.out.println("     " + s));
        System.out.println("  monitor-enter classes (lock>=1ms): " + monitors);
    }
}
