import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// WindowCpu <jfr> <from HH:MM:SS[.sss]> <to> [--vs <from2> <to2>] [--threads regex] [--top N] [--incl regex] :
// what the CPU (jdk.ExecutionSample, 1 sample = 1ms of CPU) was spent on in a window, by thread category (see
// Threads: C1/C2 JIT, GC, AccExec, MsgLoop, ...), folded thread name, the frame type of the leaf and of the first Java
// frame (Interpreted / C1 compiled / JIT compiled / Inlined: a lot of Interpreted or C1 means code not yet compiled
// by C2), leaf method, and inclusive method (frames matching --incl, default Cassandra and Accord, counted once per
// sample).
// With --vs, the same for a second window, and each row as cores in both windows, plus the cost per unit of work in
// each and their ratio, where the unit is a "DX K Applied" span (keys are sampled by hash, independent of the
// transactions traced; needs -Daccord.debug.distributed.execution.keys): e.g. warm-up --vs steady state shows which
// work got more expensive per transaction rather than just more frequent.
// CPU samples miss time spent runnable but waiting for a core: see HostPressure for that.
public class WindowCpu
{
    static final class Agg
    {
        final LocalTime from, to;
        long total, units;
        final Map<String, Long> category = new HashMap<>(), thread = new HashMap<>(), leafType = new HashMap<>(),
                                 javaType = new HashMap<>(), leaf = new HashMap<>(), inclusive = new HashMap<>();
        Agg(LocalTime from, LocalTime to) { this.from = from; this.to = to; }
        boolean contains(Instant t)
        {
            LocalTime lt = LocalTime.ofInstant(t, ZoneOffset.UTC);
            return !lt.isBefore(from) && lt.isBefore(to);
        }
        double seconds() { return Duration.between(from, to).toNanos() / 1e9; }
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length < 3) usage();
        String file = args[0];
        List<Agg> aggs = new ArrayList<>();
        aggs.add(new Agg(LocalTime.parse(args[1]), LocalTime.parse(args[2])));
        String threads = ".*", incl = "^(org\\.apache\\.cassandra|accord)\\..*";
        int top = 30;
        for (int i = 3; i < args.length; i++)
        {
            switch (args[i])
            {
                case "--vs": aggs.add(new Agg(LocalTime.parse(args[i + 1]), LocalTime.parse(args[i + 2]))); i += 2; break;
                case "--threads": threads = args[++i]; break;
                case "--top": top = Integer.parseInt(args[++i]); break;
                case "--incl": incl = args[++i]; break;
                default: usage();
            }
        }

        try (RecordingFile rf = new RecordingFile(Path.of(file)))
        {
            while (rf.hasMoreEvents())
            {
                RecordedEvent e = rf.readEvent();
                String ev = e.getEventType().getName();
                boolean sample = ev.equals("jdk.ExecutionSample");
                boolean span = !sample && ev.equals("profiler.Span");
                if (!sample && !span) continue;
                for (Agg agg : aggs)
                {
                    if (span)
                    {
                        if (agg.contains(e.getEndTime()) && e.getString("tag").startsWith("DX K Applied")) agg.units++;
                        continue;
                    }
                    if (!agg.contains(e.getStartTime())) continue;
                    String name = Threads.name(Threads.of(e));
                    if (!name.matches(threads)) continue;
                    agg.total++;
                    String category = Threads.category(e);
                    agg.category.merge(category, 1L, Long::sum);
                    // unnamed native threads (e.g. JIT compiler threads started on demand) by what they run
                    agg.thread.merge(name.startsWith("[tid=") ? "[unnamed " + category + "]" : Threads.fold(name), 1L, Long::sum);
                    RecordedStackTrace st = e.getStackTrace();
                    if (st == null || st.getFrames().isEmpty()) continue;
                    List<RecordedFrame> frames = st.getFrames();
                    RecordedFrame lf = frames.get(0);
                    agg.leafType.merge(lf.getType(), 1L, Long::sum);
                    agg.leaf.merge(method(lf) + " [" + lf.getType() + "]", 1L, Long::sum);
                    for (RecordedFrame f : frames)
                    {
                        String type = f.getType();
                        if (type.equals("Native") || type.equals("C++") || type.equals("Kernel")) continue;
                        agg.javaType.merge(type, 1L, Long::sum);
                        break;
                    }
                    Set<String> seen = new HashSet<>();
                    for (RecordedFrame f : frames)
                    {
                        String m = method(f);
                        if (m.matches(incl) && seen.add(m)) agg.inclusive.merge(m, 1L, Long::sum);
                    }
                }
            }
        }

        System.out.println(Path.of(file).getFileName() + (threads.equals(".*") ? "" : "  threads " + threads));
        for (Agg agg : aggs)
            System.out.printf("  %s-%s: %d samples = %.2f cores over %.1fs; %d K Applied (%.0f/s)%n", agg.from, agg.to,
                              agg.total, agg.total / 1000.0 / agg.seconds(), agg.seconds(), agg.units, agg.units / agg.seconds());
        print("category", aggs, a -> a.category, top);
        print("thread", aggs, a -> a.thread, top);
        print("leaf frame type", aggs, a -> a.leafType, top);
        print("first Java frame type", aggs, a -> a.javaType, top);
        print("leaf method", aggs, a -> a.leaf, top);
        print("inclusive (" + incl + ")", aggs, a -> a.inclusive, top);
    }

    static String method(RecordedFrame f)
    {
        return (f.getMethod().getType().getName() + "." + f.getMethod().getName()).replaceAll("\\$\\$Lambda[./]0x[0-9a-f]+", "\\$\\$Lambda");
    }

    static void print(String title, List<Agg> aggs, java.util.function.Function<Agg, Map<String, Long>> get, int top)
    {
        Agg a = aggs.get(0);
        Map<String, Long> m = get.apply(a);
        if (aggs.size() == 1)
        {
            System.out.println("-- " + title + "  (cores, share)");
            m.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(top)
             .forEach(x -> System.out.printf("  %7.2f %5.1f%%  %s%n", x.getValue() / 1000.0 / a.seconds(), 100.0 * x.getValue() / a.total, x.getKey()));
            return;
        }
        Agg b = aggs.get(1);
        Map<String, Long> n = get.apply(b);
        System.out.println("-- " + title + "  (cores in each window; CPU ms per K Applied in each, and their ratio)");
        Set<String> keys = new HashSet<>(m.keySet());
        keys.addAll(n.keySet());
        keys.stream().sorted((x, y) -> Long.compare(m.getOrDefault(y, 0L) + n.getOrDefault(y, 0L), m.getOrDefault(x, 0L) + n.getOrDefault(x, 0L)))
            .limit(top).forEach(k -> {
                long x = m.getOrDefault(k, 0L), y = n.getOrDefault(k, 0L);
                double ux = a.units == 0 ? Double.NaN : (double) x / a.units, uy = b.units == 0 ? Double.NaN : (double) y / b.units;
                System.out.printf("  %7.2f %7.2f   %8.2f %8.2f %6.2fx  %s%n", x / 1000.0 / a.seconds(), y / 1000.0 / b.seconds(), ux, uy, uy == 0 ? Double.NaN : ux / uy, k);
            });
    }

    static void usage()
    {
        System.err.println("usage: WindowCpu <jfr> <from HH:MM:SS[.sss]> <to> [--vs <from2> <to2>] [--threads regex] [--top N] [--incl regex]");
        System.exit(2);
    }
}
