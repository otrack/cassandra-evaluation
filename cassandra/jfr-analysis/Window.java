import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.regex.*;

// Window <file> <fromSec> <toSec> <threadRegex> <depth> [top]
// Aggregates wall (weighted by batch size) and CPU samples by state and by top-<depth> frames,
// skipping signal-handler and bare libc frames at the top of each stack.
public class Window {
    static boolean skip(String f) {
        return f.startsWith("/usr/lib/x86_64-linux-gnu/libc.so.6") || f.startsWith("./usr/lib/x86_64-linux-gnu/libc.so.6") || f.startsWith("libasyncProfiler") || f.contains("WallClock::signalHandler");
    }
    static String frame(RecordedFrame fr) {
        RecordedMethod m = fr.getMethod();
        String t = m.getType() == null ? "" : m.getType().getName();
        t = t.replaceFirst("^org\\.apache\\.cassandra\\.", "o.a.c.").replaceFirst("^java\\.util\\.concurrent\\.", "j.u.c.");
        return (t.isEmpty() ? "" : t + ".") + m.getName();
    }
    public static void main(String[] a) throws Exception {
        Path p = Path.of(a[0]); double from = Double.parseDouble(a[1]), to = Double.parseDouble(a[2]);
        Pattern thr = Pattern.compile(a[3]); int depth = Integer.parseInt(a[4]); int top = a.length > 5 ? Integer.parseInt(a[5]) : 15;
        Instant t0 = null;
        try (RecordingFile rf = new RecordingFile(p)) { while (rf.hasMoreEvents()) { Instant s = rf.readEvent().getStartTime(); if (t0 == null || s.isBefore(t0)) t0 = s; } }
        Instant A = t0.plusNanos((long)(from*1e9)), B = t0.plusNanos((long)(to*1e9));
        Map<String, Map<String, Long>> sig = new TreeMap<>();
        try (RecordingFile rf = new RecordingFile(p)) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                String type = e.getEventType().getName();
                boolean wall = type.equals("profiler.WallClockSample"), cpu = type.equals("jdk.ExecutionSample");
                if (!wall && !cpu) continue;
                Instant s = e.getStartTime(), end = wall ? s.plus(e.getDuration("timeSpan")) : s;
                if (end.isBefore(A) || s.isAfter(B)) continue;
                RecordedThread t = e.getThread("sampledThread");
                String tn = t == null ? "?" : (t.getJavaName() != null ? t.getJavaName() : t.getOSName());
                if (!thr.matcher(tn).find()) continue;
                long w = wall ? e.getLong("samples") : 1;
                List<String> kept = new ArrayList<>();
                if (e.getStackTrace() != null) for (RecordedFrame fr : e.getStackTrace().getFrames()) { String f = frame(fr); if (kept.isEmpty() && skip(f)) continue; kept.add(f); if (kept.size() >= depth) break; }
                sig.computeIfAbsent((wall ? "WALL " + e.getString("state") : "CPU"), k -> new HashMap<>()).merge(String.join(" <- ", kept), w, Long::sum);
            }
        }
        for (var en : sig.entrySet()) {
            long total = en.getValue().values().stream().mapToLong(Long::longValue).sum();
            System.out.println("## " + en.getKey() + "  total=" + total);
            en.getValue().entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(top)
              .forEach(x -> System.out.printf("  %7d %5.1f%%  %s%n", x.getValue(), 100.0 * x.getValue() / total, x.getKey()));
        }
    }
}
