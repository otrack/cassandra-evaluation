import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
// RequestLatency <jfr> [longMs]: server-side client request latency, reconstructed from wall samples of SharedPool workers
// parked in AccordResult.awaitAndGet (reads and writes). Each park starts with an
// unbatched wall sample (samples=1); subsequent batched samples extend it. Duration = first sample -> end of last
// (a lower bound, ~10ms resolution). Prints read/write percentiles and requests > longMs; CSV=1 instead prints
// "startEpochMs,durationMs,kind" for every request (input to xcorr.py).
public class RequestLatency {
    record S(long t, long end, long n, boolean in, boolean sleeping, String kind) {}
    static long ns(Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    public static void main(String[] a) throws Exception {
        long longMs = a.length > 1 ? Long.parseLong(a[1]) : 1000;
        Map<String, List<S>> m = new HashMap<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals("profiler.WallClockSample")) continue;
                RecordedThread th = e.getThread("sampledThread");
                if (th == null || th.getJavaName() == null || !th.getJavaName().startsWith("SharedPool-Worker")) continue;
                boolean in = false; String kind = "?";
                RecordedStackTrace st = e.getStackTrace();
                if (st != null) for (RecordedFrame f : st.getFrames()) {
                    String n = f.getMethod().getType().getName() + "." + f.getMethod().getName();
                    if (n.endsWith("AccordResult.awaitAndGet")) in = true;
                    if (in && kind.equals("?") && (n.contains("SelectStatement") || n.contains("ModificationStatement"))) kind = n.contains("Select") ? "read" : "write";
                }
                long t = ns(e.getStartTime());
                m.computeIfAbsent(th.getJavaName(), k -> new ArrayList<>()).add(new S(t, t + e.getDuration("timeSpan").toNanos(), e.getLong("samples"), in, e.getString("state").contains("SLEEP"), kind));
            }
        }
        List<long[]> rq = new ArrayList<>(); List<String> kinds = new ArrayList<>(); List<String> thr = new ArrayList<>();
        for (var en : m.entrySet()) {
            List<S> l = en.getValue(); l.sort(Comparator.comparingLong(S::t));
            long s = -1, e = -1; String k = "";
            for (S x : l) {
                boolean newPark = x.n() == 1;
                if (s >= 0 && (!x.in() || newPark)) { rq.add(new long[]{s, e}); kinds.add(k); thr.add(en.getKey()); s = -1; }
                if (x.in()) { if (s < 0) { s = x.t(); k = x.kind(); } e = Math.max(e, x.end()); }
            }
            if (s >= 0) { rq.add(new long[]{s, e}); kinds.add(k); thr.add(en.getKey()); }
        }
        if (System.getenv("CSV") != null) { for (int i = 0; i < rq.size(); i++) System.out.println(rq.get(i)[0]/1_000_000 + "," + (rq.get(i)[1]-rq.get(i)[0])/1_000_000 + "," + kinds.get(i)); return; }
        String node = Path.of(a[0]).getFileName().toString().replaceAll(".*_","").replace(".jfr","");
        for (String kk : new TreeSet<>(kinds)) {
            long[] d = new long[0]; List<Long> l = new ArrayList<>();
            for (int i = 0; i < rq.size(); i++) if (kinds.get(i).equals(kk)) l.add((rq.get(i)[1]-rq.get(i)[0])/1_000_000);
            d = l.stream().mapToLong(x->x).sorted().toArray();
            int[] h = new int[30]; for (long v : d) h[(int)Math.min(29, v/50)]++;
            System.out.printf("%-10s %-5s n=%5d ms p50=%d p90=%d p99=%d p999=%d max=%d >%dms=%d  hist50ms=%s%n", node, kk, d.length, d[d.length/2], d[d.length*9/10], d[d.length*99/100], d[(int)(d.length*0.999)], d[d.length-1], longMs, Arrays.stream(d).filter(x->x>longMs).count(), Arrays.toString(h).replace(" ",""));
        }
        List<Integer> idx = new ArrayList<>(); for (int i = 0; i < rq.size(); i++) if ((rq.get(i)[1]-rq.get(i)[0])/1_000_000 > longMs) idx.add(i);
        idx.sort(Comparator.comparingLong(i -> rq.get(i)[0]));
        for (int i : idx) System.out.printf("   %s -> %s %5dms %s %s%n", LocalTime.ofInstant(Instant.ofEpochSecond(0, rq.get(i)[0]), ZoneOffset.UTC), LocalTime.ofInstant(Instant.ofEpochSecond(0, rq.get(i)[1]), ZoneOffset.UTC), (rq.get(i)[1]-rq.get(i)[0])/1_000_000, kinds.get(i), thr.get(i));
    }
}
