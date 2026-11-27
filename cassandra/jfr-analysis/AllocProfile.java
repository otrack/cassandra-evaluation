import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.regex.*;

// AllocProfile [--from HH:MM:SS[.sss]] [--to ...] [--threads regex] [--top N] [--site regex] [--vs <from2> <to2>] <jfr> :
// where the allocation rate comes from, from the allocation samples of cassandra.profiler.alloc (async-profiler
// alloc=<bytes>: jdk.ObjectAllocationInNewTLAB / jdk.ObjectAllocationOutsideTLAB) or of the JDK's own
// jdk.ObjectAllocationSample. Each sample stands for the bytes it is weighted by (see CallTree), so the sums are
// allocation rates (MB/s), to be compared with what the GC has to collect (see the GC log's young collections):
//   - by thread category (Threads.category) and folded thread name
//   - by allocated class
//   - by allocation site: the innermost frame matching --site (default: Cassandra/Accord code, i.e. the code that
//     asked for the allocation, rather than the JDK collection or buffer doing it), with the class allocated there
//   - by inclusive method (Cassandra/Accord frames, counted once per sample): which operations allocate the most
// With --vs, the same for a second window side by side. For the full call paths of one operation, use
// CallTree --event alloc --anchor <method>.
public class AllocProfile
{
    static final class Agg
    {
        final LocalTime from, to;
        Instant first, last;
        long total, samples;
        final Map<String, Long> category = new HashMap<>(), thread = new HashMap<>(), cls = new HashMap<>(),
                                 site = new HashMap<>(), inclusive = new HashMap<>();
        Agg(LocalTime from, LocalTime to) { this.from = from; this.to = to; }
        boolean contains(Instant t)
        {
            if (from == null) return true;
            LocalTime lt = LocalTime.ofInstant(t, ZoneOffset.UTC);
            return !lt.isBefore(from) && lt.isBefore(to);
        }
        double seconds()
        {
            if (from != null) return Duration.between(from, to).toNanos() / 1e9;
            return first == null ? 1 : Math.max(0.001, Duration.between(first, last).toNanos() / 1e9);
        }
    }

    public static void main(String[] args) throws Exception
    {
        String file = null;
        LocalTime from = null, to = null;
        Agg vs = null;
        Pattern threads = null, site = Pattern.compile("^(org\\.apache\\.cassandra|accord)\\.");
        int top = 25;
        for (int i = 0; i < args.length; i++)
        {
            switch (args[i])
            {
                case "--from": from = LocalTime.parse(args[++i]); break;
                case "--to": to = LocalTime.parse(args[++i]); break;
                case "--vs": vs = new Agg(LocalTime.parse(args[i + 1]), LocalTime.parse(args[i + 2])); i += 2; break;
                case "--threads": threads = Pattern.compile(args[++i]); break;
                case "--site": site = Pattern.compile(args[++i]); break;
                case "--top": top = Integer.parseInt(args[++i]); break;
                default: file = args[i];
            }
        }
        if (file == null || (from == null) != (to == null))
        {
            System.err.println("usage: AllocProfile [--from HH:MM:SS[.sss] --to HH:MM:SS[.sss]] [--threads regex] [--top N] [--site regex] [--vs <from2> <to2>] <jfr>");
            System.exit(2);
        }
        List<Agg> aggs = new ArrayList<>();
        aggs.add(new Agg(from, to));
        if (vs != null) aggs.add(vs);

        try (RecordingFile rf = new RecordingFile(Path.of(file)))
        {
            while (rf.hasMoreEvents())
            {
                RecordedEvent e = rf.readEvent();
                long w = CallTree.weight(e, "alloc");
                if (w <= 0) continue;
                for (Agg agg : aggs)
                {
                    Instant t = e.getStartTime();
                    if (!agg.contains(t)) continue;
                    String name = Threads.name(Threads.of(e));
                    if (threads != null && !threads.matcher(name).find()) continue;
                    if (agg.first == null || t.isBefore(agg.first)) agg.first = t;
                    if (agg.last == null || t.isAfter(agg.last)) agg.last = t;
                    agg.total += w;
                    agg.samples++;
                    agg.category.merge(Threads.category(e), w, Long::sum);
                    agg.thread.merge(Threads.fold(name), w, Long::sum);
                    String c = className(e);
                    agg.cls.merge(c, w, Long::sum);
                    RecordedStackTrace st = e.getStackTrace();
                    if (st == null) continue;
                    String s = null;
                    Set<String> seen = new HashSet<>();
                    for (RecordedFrame f : st.getFrames())
                    {
                        String full = f.getMethod().getType().getName() + "." + f.getMethod().getName();
                        if (!site.matcher(full).find()) continue;
                        String simple = CallTree.simple(f);
                        if (s == null) s = simple + ":" + f.getLineNumber();
                        if (seen.add(simple)) agg.inclusive.merge(simple, w, Long::sum);
                    }
                    agg.site.merge((s == null ? "(no matching frame)" : s) + "  " + c, w, Long::sum);
                }
            }
        }

        System.out.println(Path.of(file).getFileName() + (threads == null ? "" : "  threads " + threads.pattern()));
        boolean any = false;
        for (Agg a : aggs)
        {
            any |= a.samples > 0;
            System.out.printf("  %s: %d samples, %.0f MB/s over %.1fs%n", a.from == null ? "whole recording" : a.from + "-" + a.to,
                              a.samples, a.total / 1e6 / a.seconds(), a.seconds());
        }
        if (!any)
        {
            System.out.println("no allocation samples: was the capture taken with cassandra.profiler.alloc (async-profiler alloc=<bytes>)?");
            return;
        }
        print("thread category", aggs, a -> a.category, top);
        print("thread", aggs, a -> a.thread, top);
        print("class", aggs, a -> a.cls, top);
        print("allocation site (innermost " + site.pattern() + " frame) and class", aggs, a -> a.site, top);
        print("inclusive method", aggs, a -> a.inclusive, top);
    }

    static String className(RecordedEvent e)
    {
        RecordedClass c = e.hasField("objectClass") ? e.getClass("objectClass") : null;
        return c == null ? "?" : c.getName();
    }

    static void print(String title, List<Agg> aggs, java.util.function.Function<Agg, Map<String, Long>> get, int top)
    {
        Agg a = aggs.get(0);
        Agg b = aggs.size() > 1 ? aggs.get(1) : null;
        Map<String, Long> m = get.apply(a), n = b == null ? Map.of() : get.apply(b);
        System.out.println("-- " + title + (b == null ? "  (MB/s, share)" : "  (MB/s and share in each window)"));
        Set<String> keys = new HashSet<>(m.keySet());
        keys.addAll(n.keySet());
        keys.stream().sorted((x, y) -> Long.compare(m.getOrDefault(y, 0L) + n.getOrDefault(y, 0L), m.getOrDefault(x, 0L) + n.getOrDefault(x, 0L)))
            .limit(top).forEach(k -> {
                long x = m.getOrDefault(k, 0L);
                if (b == null)
                {
                    System.out.printf("  %8.1f %5.1f%%  %s%n", x / 1e6 / a.seconds(), 100.0 * x / a.total, k);
                }
                else
                {
                    long y = n.getOrDefault(k, 0L);
                    System.out.printf("  %8.1f %5.1f%%   %8.1f %5.1f%%  %s%n", x / 1e6 / a.seconds(), a.total == 0 ? 0 : 100.0 * x / a.total,
                                      y / 1e6 / b.seconds(), b.total == 0 ? 0 : 100.0 * y / b.total, k);
                }
            });
    }
}
