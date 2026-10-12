import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// ExecutorStates <jfr> <from> <to>: classify AccordExecutor wall+cpu samples: lockwait / idle(awaitExclusive) / running task kind (frame below AbstractLockLoop$2.run) / in critical section
public class ExecutorStates {
    public static void main(String[] a) throws Exception {
        LocalTime from = LocalTime.parse(a[1]), to = LocalTime.parse(a[2]);
        Map<String,Long> m = new TreeMap<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); String ev = e.getEventType().getName();
                if (!ev.equals("profiler.WallClockSample")) continue;
                LocalTime t = LocalTime.ofInstant(e.getStartTime(), ZoneOffset.UTC);
                if (t.isBefore(from) || !t.isBefore(to)) continue;
                RecordedThread th = e.getThread("sampledThread");
                if (th == null || th.getJavaName() == null || !th.getJavaName().startsWith("AccordExecutor")) continue;
                List<RecordedFrame> fr = e.getStackTrace().getFrames();
                String cls = "other";
                for (int i = 0; i < fr.size(); i++) {
                    String n = fr.get(i).getMethod().getType().getName() + "." + fr.get(i).getMethod().getName();
                    if (n.endsWith("awaitExclusive")) { cls = "idle"; break; }
                    if (n.endsWith("AccordExecutor.lock") || n.endsWith("AbstractLockLoop.lock")) { cls = "lockwait"; break; }
                    if (n.endsWith("ExclusiveExecutor.runTask") || n.contains("ExclusiveExecutor.lockAndRun")) { 
                        // parked waiting for the store owner?
                        for (int j = 0; j < i; j++) { String m2 = fr.get(j).getMethod().getType().getName()+"."+fr.get(j).getMethod().getName(); if (m2.endsWith("LockSupport.park")) { cls = "storewait"; break; } }
                        if (cls.equals("other")) cls = "run-store"; break; }
                    if (n.endsWith("AbstractLockLoop$2.run")) {
                        String below = i > 0 ? fr.get(i-1).getMethod().getType().getName().replaceAll(".*\\.","") + "." + fr.get(i-1).getMethod().getName() : "?";
                        cls = "loop:" + below; break; }
                }
                m.merge(cls + " " + e.getString("state").replace("STATE_",""), e.getLong("samples"), Long::sum);
            }
        }
        System.out.println(a[1] + "-" + a[2] + " " + m);
    }
}
