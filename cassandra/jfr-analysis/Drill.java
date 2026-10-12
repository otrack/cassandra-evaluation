import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// Drill <file> <anchorSubstring> <n> [filterSubstring] : wall RUNNABLE samples on executor threads whose stack contains anchor;
// show the chain of accord/cassandra frames from the anchor (exclusive) to the leaf-most accord/cassandra frame, plus leaf
public class Drill {
    public static void main(String[] a) throws Exception {
        Map<String, Long> m = new HashMap<>(); long tot = 0; String filter = a.length > 3 ? a[3] : null;
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals("profiler.WallClockSample") || !e.getString("state").contains("RUNNABLE") || e.getStackTrace() == null) continue;
                RecordedThread t = e.getThread("sampledThread"); if (t == null || t.getJavaName() == null || !t.getJavaName().startsWith("AccordExecutor")) continue;
                List<RecordedFrame> fs = e.getStackTrace().getFrames(); int an = -1;
                for (int i = 0; i < fs.size(); i++) { String n = fs.get(i).getMethod().getType().getName() + "." + fs.get(i).getMethod().getName(); if (n.contains(a[1])) an = i; }
                if (an < 0) continue;
                if (filter != null) { boolean ok = false; for (int i = 0; i < an; i++) if ((fs.get(i).getMethod().getType().getName() + "." + fs.get(i).getMethod().getName()).matches(".*(" + filter + ").*")) ok = true; if (!ok) continue; }
                tot++;
                List<String> chain = new ArrayList<>();
                for (int i = an - 1; i >= 0; i--) { String tn = fs.get(i).getMethod().getType().getName();
                    if (tn.startsWith("accord.") || tn.startsWith("org.apache.cassandra.")) chain.add(tn.substring(tn.lastIndexOf('.') + 1) + "." + fs.get(i).getMethod().getName()); }
                // collapse to the first 4 and last 3 frames of the chain
                List<String> c = chain.size() <= 7 ? chain : new ArrayList<>(chain.subList(0, 4)); if (chain.size() > 7) { c.add("…"); c.addAll(chain.subList(chain.size() - 3, chain.size())); }
                String leaf = fs.get(0).getMethod().getType().getName().replaceAll(".*\\.", "") + "." + fs.get(0).getMethod().getName();
                m.merge(String.join(" > ", c) + "   {leaf " + leaf + "}", 1L, Long::sum);
            }
        }
        final long T = tot; System.out.println("samples: " + T + " (= " + T / 100.0 + " thread-seconds)");
        m.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(Integer.parseInt(a[2])).forEach(x -> System.out.printf("  %5.1f%%  %s%n", 100.0 * x.getValue() / T, x.getKey()));
    }
}
