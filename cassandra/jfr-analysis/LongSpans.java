import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// LongSpans <thresholdMs> <file>... : spans longer than the threshold, grouped into clusters of overlapping spans.
// Independent slow tasks give many small clusters; a shared stall (GC, safepoint, a lock) gives few clusters with many
// threads whose spans END together (small "ends spread").
public class LongSpans
{
    record S(Instant start, Instant end, String tag, String thread) {}

    public static void main(String[] args) throws Exception
    {
        long thresholdMs = Long.parseLong(args[0]);
        for (int i = 1; i < args.length; i++)
        {
            List<S> spans = new ArrayList<>();
            Instant t0 = null;
            try (RecordingFile rf = new RecordingFile(Path.of(args[i])))
            {
                while (rf.hasMoreEvents())
                {
                    RecordedEvent e = rf.readEvent();
                    if (t0 == null || e.getStartTime().isBefore(t0)) t0 = e.getStartTime();
                    if (!e.getEventType().getName().equals("profiler.Span")) continue;
                    if (e.getDuration().toMillis() < thresholdMs) continue;
                    String tag = e.getString("tag");
                    if (Tags.isDx(tag)) continue; // per-transaction spans, not executor work
                    String kind = Tags.kind(tag);
                    String folded = kind == null ? tag : (Tags.isQueued(tag) ? "Queued " : Tags.isHead(tag) ? "Head " : "") + kind;
                    spans.add(new S(e.getStartTime(), e.getEndTime(), folded, e.getThread() == null ? "?" : e.getThread().getJavaName()));
                }
            }
            spans.sort(Comparator.comparing(S::start));
            System.out.println("== " + Path.of(args[i]).getFileName() + "  (recording start " + t0 + ", " + spans.size() + " spans >= " + thresholdMs + "ms)");
            Instant clusterEnd = null; int cluster = 0;
            Map<Integer, List<S>> clusters = new LinkedHashMap<>();
            for (S s : spans)
            {
                if (clusterEnd == null || s.start().isAfter(clusterEnd)) cluster++;
                clusterEnd = clusterEnd == null || s.end().isAfter(clusterEnd) ? s.end() : clusterEnd;
                clusters.computeIfAbsent(cluster, k -> new ArrayList<>()).add(s);
            }
            final Instant base = t0;
            for (var c : clusters.entrySet())
            {
                List<S> l = c.getValue();
                Instant cs = l.get(0).start(), ce = l.stream().map(S::end).max(Comparator.naturalOrder()).get();
                Instant firstEnd = l.stream().map(S::end).min(Comparator.naturalOrder()).get();
                Set<String> threads = new TreeSet<>(); Map<String, Integer> tags = new TreeMap<>();
                for (S s : l) { threads.add(s.thread()); tags.merge(s.tag(), 1, Integer::sum); }
                System.out.printf("  cluster %d: +%.3fs..+%.3fs  starts spread %.0fms, ends spread %.0fms  n=%d threads=%d %s%n",
                                  c.getKey(), Duration.between(base, cs).toNanos() / 1e9, Duration.between(base, ce).toNanos() / 1e9,
                                  Duration.between(cs, l.get(l.size() - 1).start()).toNanos() / 1e6, Duration.between(firstEnd, ce).toNanos() / 1e6,
                                  l.size(), threads.size(), tags);
            }
        }
    }
}
