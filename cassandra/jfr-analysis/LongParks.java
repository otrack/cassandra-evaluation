// LongParks <jfr> : distinct threads per pool, and jdk.ThreadPark / JavaMonitorEnter events >= -Dms=<ms> (default 50) on
// Messaging/Native/AccordExecutor/SharedPool/epoll threads, with their stacks.
import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
public class LongParks {
    public static void main(String[] a) throws Exception {
        Map<String,Set<Long>> m = new TreeMap<>(); List<String> locks = new ArrayList<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); String ev = e.getEventType().getName();
                RecordedThread th = e.hasField("sampledThread") ? e.getThread("sampledThread") : e.getThread();
                if (th != null) { String n = th.getJavaName() == null ? "[native]" : th.getJavaName().replaceAll("\\d+","N"); m.computeIfAbsent(n, k->new HashSet<>()).add(th.getOSThreadId()); }
                if ((ev.equals("jdk.JavaMonitorEnter") || ev.equals("jdk.ThreadPark")) && e.getDuration().toMillis() >= Long.getLong("ms", 50)) {
                    String tn = th == null ? "?" : String.valueOf(th.getJavaName());
                    if (tn.matches("(Messaging|Native|AccordExec|SharedPool|epoll).*")) {
                        StringBuilder sb = new StringBuilder(); int i=0; for (RecordedFrame f : e.getStackTrace().getFrames()) { if (i++ > 7) break; sb.append(" < ").append(f.getMethod().getType().getName().replaceAll(".*\\.","")).append('.').append(f.getMethod().getName()); }
                        locks.add(String.format("%s %s %dms %s%s", e.getStartTime(), ev, e.getDuration().toMillis(), tn, sb));
                    }
                }
            }
        }
        m.forEach((k,v) -> { if (v.size() > 2) System.out.println("  " + v.size() + " " + k); });
        System.out.println("long park/monitor on app threads: " + locks.size()); locks.stream().limit(15).forEach(System.out::println);
    }
}
