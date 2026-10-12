// CpuByThread <jfr>... : per second, CPU samples by thread category (AccExec, MsgLoop, JIT, GC, NTR, Shared, Journal, ...);
// JIT/GC are recognised by stack (CompileBroker / ZGC). QUIET=1 prints only the per-file totals.
import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

public class CpuByThread {
    static String cat(String t) {
        if (t == null) return "?";
        if (t.startsWith("AccordExecutor")) return "AccExec";
        if (t.startsWith("Messaging-EventLoop")) return "MsgLoop";
        if (t.contains("CompilerThread") || t.startsWith("C2") || t.startsWith("C1")) return "JIT";
        if (t.startsWith("Z") || t.startsWith("GC")) return "GC";
        if (t.startsWith("Native-Transport") || t.contains("nioEventLoop")|| t.contains("epollEventLoop")) return "NTR";
        if (t.startsWith("SharedPool")) return "Shared";
        if (t.contains("Journal")) return "Journal";
        if (t.startsWith("VM Thread")) return "VMThr";
        return "other";
    }
    public static void main(String[] a) throws Exception {
        for (String f : a) {
            Instant t0 = null;
            TreeMap<Long, Map<String,Integer>> m = new TreeMap<>();
            Map<String,Integer> otherNames = new HashMap<>(); Map<String,Integer> tot = new TreeMap<>();
            try (RecordingFile rf = new RecordingFile(Path.of(f))) {
                while (rf.hasMoreEvents()) {
                    RecordedEvent e = rf.readEvent();
                    if (!e.getEventType().getName().equals("jdk.ExecutionSample")) continue;
                    RecordedThread th = e.getThread("sampledThread");
                    String n = th == null ? null : (th.getJavaName() != null ? th.getJavaName() : th.getOSName());
                    long s = e.getStartTime().getEpochSecond();
                    String c = cat(n);
                    RecordedStackTrace st = e.getStackTrace();
                    if (st != null) for (RecordedFrame fr : st.getFrames()) { String mn = fr.getMethod().getType().getName()+"."+fr.getMethod().getName(); if (mn.contains("CompileBroker")) { c = "JIT"; break; } if (mn.contains("ZDriver")||mn.contains("ZWorker")||mn.contains("ZDirector")) { c="GC"; break; } }
                    tot.merge(c, 1, Integer::sum);
                    if (c.equals("other")) otherNames.merge(n.replaceAll("\\d+","N"), 1, Integer::sum);
                    m.computeIfAbsent(s, k -> new TreeMap<>()).merge(c, 1, Integer::sum);
                }
            }
            System.out.println("== " + Path.of(f).getFileName() + " TOTAL " + tot); if (System.getenv("QUIET")!=null) continue;
            for (var en : m.entrySet()) System.out.println("  " + LocalTime.ofSecondOfDay(en.getKey() % 86400) + " " + en.getValue());
            otherNames.entrySet().stream().sorted((x,y)->y.getValue()-x.getValue()).limit(15).forEach(x -> System.out.println("   other: " + x));
        }
    }
}
