import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// Phases <file> <threadPrefix> [fromSec toSec] : classify executor samples by the callee of the loop body
// (AbstractLockLoop$N.run), then show the hottest frames below it per phase.
public class Phases {
    static String fr(RecordedFrame f) { String t = f.getMethod().getType().getName(); return t.substring(t.lastIndexOf('.') + 1) + "." + f.getMethod().getName(); }
    public static void main(String[] a) throws Exception {
        Path p = Path.of(a[0]); String prefix = a[1];
        double from = a.length > 2 ? Double.parseDouble(a[2]) : 0, to = a.length > 3 ? Double.parseDouble(a[3]) : 1e9;
        Instant t0 = null;
        try (RecordingFile rf = new RecordingFile(p)) { while (rf.hasMoreEvents()) { Instant s = rf.readEvent().getStartTime(); if (t0 == null || s.isBefore(t0)) t0 = s; } }
        Instant A = t0.plusNanos((long)(from*1e9)), B = t0.plusNanos((long)(to*1e9));
        Map<String, Map<String, Long>> phase = new TreeMap<>();            // kind -> phase -> weight
        Map<String, Map<String, Long>> detail = new HashMap<>();           // kind|phase -> frames -> weight
        try (RecordingFile rf = new RecordingFile(p)) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                String type = e.getEventType().getName();
                boolean wall = type.equals("profiler.WallClockSample"), cpu = type.equals("jdk.ExecutionSample");
                if (!wall && !cpu) continue;
                if (e.getStartTime().isBefore(A) || e.getStartTime().isAfter(B)) continue;
                RecordedThread t = e.getThread("sampledThread");
                if (t == null || t.getJavaName() == null || !t.getJavaName().startsWith(prefix)) continue;
                String kind = cpu ? "CPU" : "WALL " + e.getString("state").replace("STATE_", "");
                long w = wall ? e.getLong("samples") : 1;
                List<RecordedFrame> fs = e.getStackTrace() == null ? List.of() : e.getStackTrace().getFrames();
                int loop = -1;
                for (int i = 0; i < fs.size(); i++) if (fr(fs.get(i)).matches("AbstractLockLoop\\$\\d+\\.run")) { loop = i; break; }
                String ph = loop <= 0 ? "(outside loop)" : fr(fs.get(loop - 1));
                // refine the run phase by the next couple of frames below Task.runNoExcept
                phase.computeIfAbsent(kind, k -> new HashMap<>()).merge(ph, w, Long::sum);
                // detail: the top cassandra/accord frames (leaf-most 3 non-JDK frames) within this phase
                List<String> d = new ArrayList<>();
                for (int i = 0; i < (loop < 0 ? fs.size() : loop) && d.size() < 3; i++) {
                    String tn = fs.get(i).getMethod().getType().getName();
                    if (tn.startsWith("accord.") || tn.startsWith("org.apache.cassandra.")) d.add(fr(fs.get(i)));
                }
                detail.computeIfAbsent(kind + "|" + ph, k -> new HashMap<>()).merge(String.join(" <- ", d), w, Long::sum);
            }
        }
        for (var k : phase.entrySet()) {
            long tot = k.getValue().values().stream().mapToLong(Long::longValue).sum();
            System.out.println("## " + k.getKey() + " total=" + tot);
            k.getValue().entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(8).forEach(x -> {
                System.out.printf("  %5.1f%%  %s%n", 100.0 * x.getValue() / tot, x.getKey());
                if (!k.getKey().contains("SLEEPING"))
                    detail.get(k.getKey() + "|" + x.getKey()).entrySet().stream().sorted((u, v) -> Long.compare(v.getValue(), u.getValue())).limit(4)
                        .forEach(u -> System.out.printf("           %5.1f%%  %s%n", 100.0 * u.getValue() / tot, u.getKey().isEmpty() ? "(JDK/native only)" : u.getKey()));
            });
        }
    }
}
