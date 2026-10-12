import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// Activity <bucketMs> <from> <to> <jfr>... : per bucket, per node: CPU samples on app threads (AccordExecutor/Messaging/Native/SharedPool) + wall RUNNABLE samples on those threads
public class Activity {
    public static void main(String[] a) throws Exception {
        long b = Long.parseLong(a[0]); LocalTime from = LocalTime.parse(a[1]), to = LocalTime.parse(a[2]);
        int n = a.length - 3; TreeMap<Long,int[]> m = new TreeMap<>();
        for (int i = 0; i < n; i++) try (RecordingFile rf = new RecordingFile(Path.of(a[i+3]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                String ev = e.getEventType().getName();
                if (!ev.equals("jdk.ExecutionSample")) continue;
                LocalTime t = LocalTime.ofInstant(e.getStartTime(), ZoneOffset.UTC);
                if (t.isBefore(from) || !t.isBefore(to)) continue;
                RecordedThread th = e.getThread("sampledThread");
                String nm = th == null || th.getJavaName() == null ? "" : th.getJavaName();
                if (!nm.matches("(AccordExecutor|Messaging-EventLoop|Native-Transport|SharedPool|epollEventLoop).*")) continue;
                long k = (e.getStartTime().toEpochMilli()) / b;
                m.computeIfAbsent(k, x -> new int[n])[i]++;
            }
        }
        long first = m.firstKey(), last = m.lastKey();
        for (long k = first; k <= last; k++) { int[] v = m.getOrDefault(k, new int[n]); StringBuilder sb = new StringBuilder(LocalTime.ofInstant(Instant.ofEpochMilli(k*b), ZoneOffset.UTC).toString()); int s=0; for (int x : v) { sb.append(String.format(" %4d", x)); s+=x; } sb.append("  ").append("#".repeat(Math.min(60,s))); System.out.println(sb); }
    }
}
