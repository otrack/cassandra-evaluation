import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// TopStacks <jfr> <threadRegex> <depth> [eventType] : top stacks (top N frames) for matching threads
public class TopStacks {
    public static void main(String[] a) throws Exception {
        String re = a[1]; int depth = Integer.parseInt(a[2]); String ev = a.length > 3 ? a[3] : "jdk.ExecutionSample";
        Map<String,Integer> m = new HashMap<>(); Map<String,Integer> names = new HashMap<>(); int tot=0;
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals(ev)) continue;
                RecordedThread th = e.getThread("sampledThread");
                String n = th == null ? "?" : (th.getJavaName() != null ? th.getJavaName() : th.getOSName());
                if (n == null) n = "[tid=" + th.getOSThreadId() + "]";
                if (!n.matches(re)) continue;
                tot++; names.merge(n, 1, Integer::sum);
                StringBuilder sb = new StringBuilder();
                RecordedStackTrace st = e.getStackTrace();
                if (st != null) { int i=0; for (RecordedFrame fr : st.getFrames()) { if (i++>=depth) break; sb.append("\n      ").append(fr.getMethod().getType().getName()).append('.').append(fr.getMethod().getName()); } }
                m.merge(sb.toString(), 1, Integer::sum);
            }
        }
        System.out.println("total " + tot);
        names.entrySet().stream().sorted((x,y)->y.getValue()-x.getValue()).limit(10).forEach(x->System.out.println("  thread " + x));
        m.entrySet().stream().sorted((x,y)->y.getValue()-x.getValue()).limit(12).forEach(x->System.out.println("  " + x.getValue() + x.getKey()));
    }
}
