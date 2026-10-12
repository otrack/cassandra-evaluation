import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// Cfk <file> : wall RUNNABLE samples in store tasks touching accord.local.cfk, grouped by innermost cfk method and by
// whether the leaf is a Timestamp/TxnId comparison (i.e. a scan/search over the key's transaction list)
public class Cfk {
    public static void main(String[] a) throws Exception {
        Map<String, Long> inner = new HashMap<>(), caller = new HashMap<>(); long tot = 0, cmp = 0;
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals("profiler.WallClockSample") || !e.getString("state").contains("RUNNABLE") || e.getStackTrace() == null) continue;
                RecordedThread t = e.getThread("sampledThread"); if (t == null || t.getJavaName() == null || !t.getJavaName().startsWith("AccordExecutor")) continue;
                List<String> n = new ArrayList<>(); for (RecordedFrame f : e.getStackTrace().getFrames()) n.add(f.getMethod().getType().getName() + "." + f.getMethod().getName());
                int st = -1; for (int i = 0; i < n.size(); i++) if (n.get(i).endsWith("SafeTask.runMayThrow")) { st = i; break; }
                if (st < 0) continue;
                int in = -1; for (int i = 0; i < st; i++) if (n.get(i).startsWith("accord.local.cfk.")) { in = i; break; }
                if (in < 0) continue;
                // skip if journal/updateSize ancestors (counted elsewhere)
                boolean other = false; for (int i = 0; i < st; i++) if (n.get(i).matches(".*(journal\\.|AccordJournal|estimateHeapSize|AccordObjectSizes).*")) other = true; if (other) continue;
                tot++;
                inner.merge(n.get(in).replace("accord.local.cfk.", ""), 1L, Long::sum);
                String leaf = n.get(0); if (leaf.matches(".*(Timestamp|TxnId)\\.compareTo|.*SortedArrays.*|.*binarySearch.*")) cmp++;
                String c = "?"; for (int i = st - 1; i >= 0; i--) if (n.get(i).startsWith("accord.messages.") || n.get(i).startsWith("accord.coordinate.")) { c = n.get(i).replaceAll("^accord\\.", "").replaceAll("\\$.*", ""); break; }
                caller.merge(c, 1L, Long::sum);
            }
        }
        final long T = tot;
        System.out.printf("%s: %d samples (%.1f thread-s); leaf is a Timestamp/TxnId comparison or array search in %.0f%%%n", Path.of(a[0]).getFileName(), T, T / 100.0, 100.0 * cmp / T);
        System.out.println("  innermost CommandsForKey method:");
        inner.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(10).forEach(x -> System.out.printf("    %5.1f%%  %s%n", 100.0 * x.getValue() / T, x.getKey()));
        System.out.println("  by protocol step:");
        caller.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(8).forEach(x -> System.out.printf("    %5.1f%%  %s%n", 100.0 * x.getValue() / T, x.getKey()));
    }
}
