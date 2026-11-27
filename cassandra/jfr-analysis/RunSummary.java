import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.regex.*;

// RunSummary [--from HH:MM:SS[.sss] --to ...] <jfr>... : one line per capture, over the run's measured window, to
// compare the points of a sweep (e.g. all the *_Ashburn1.jfr of a saturation run):
//   window     by default found from executor activity: YCSB stops between its warm-up and the measured phase, so
//              the window is the longest stretch of activity without a gap of >= 2s, less its first 5s (the clients'
//              start-up burst) and its last second; --from/--to override it (the same window for every file)
//   KApp/s     "DX K Applied" spans per second (throughput proxy: keys sampled by hash, see WorkRate)
//   exCores    executor CPU (cores); node%: jdk.CPULoad machineTotal
//   Client/PreAcc/Execute  p50 of the coordinator's "DX C" spans (ms) for transactions coordinated here
//   deps50     p50 of deps= on "DX E Executing"; nonfast%: share of those not on the FAST path
//   Qp99       p99 of "DX Q" (queued) spans of traced transactions (ms)
//   inflight   mean/max SharedPool workers parked in AccordResult.awaitAndGet, on a 50ms grid: transactions in flight
//              coordinated here, which native_transport_max_threads caps
// DX spans need -Daccord.debug.distributed.execution[.keys]=N; latencies are of the traced (1 in N) transactions.
public class RunSummary
{
    static final Pattern DEPS = Pattern.compile("^DX E Executing \\S+ (\\S+) deps=(\\d+)");

    public static void main(String[] args) throws Exception
    {
        Dx.Window fixed = new Dx.Window();
        List<String> files = new ArrayList<>();
        for (int i = 0; i < args.length; )
        {
            int n = fixed.accept(args, i);
            if (n > 0) i += n;
            else files.add(args[i++]);
        }
        if (files.isEmpty())
        {
            System.err.println("usage: RunSummary [--from HH:MM:SS[.sss] --to HH:MM:SS[.sss]] <jfr>...");
            System.exit(2);
        }
        System.out.println("file                                      window               KApp/s exCores node%  Client PreAcc Execute deps50 nonfast%  Qp99  inflight(mean/max)");
        for (String file : files)
            summarise(file, fixed);
    }

    static void summarise(String file, Dx.Window fixed) throws Exception
    {
        Instant from, to;
        if (fixed.any())
        {
            from = null; to = null;
        }
        else
        {
            TreeMap<Long, Integer> cpu = new TreeMap<>();
            try (RecordingFile rf = new RecordingFile(Path.of(file)))
            {
                while (rf.hasMoreEvents())
                {
                    RecordedEvent e = rf.readEvent();
                    if (!e.getEventType().getName().equals("jdk.ExecutionSample")) continue;
                    if (Threads.name(Threads.of(e)).startsWith("AccordExecutor")) cpu.merge(e.getStartTime().getEpochSecond(), 1, Integer::sum);
                }
            }
            // active seconds: executor CPU above a fifth of the busiest second's (and some minimum); the measured
            // phase is the longest run of active seconds with no gap of 2s or more
            int peak = cpu.values().stream().mapToInt(Integer::intValue).max().orElse(0);
            int threshold = Math.max(20, peak / 5);
            List<Long> active = new ArrayList<>();
            for (var en : cpu.entrySet()) if (en.getValue() > threshold) active.add(en.getKey());
            if (active.isEmpty())
            {
                System.out.println(Path.of(file).getFileName() + ": no executor activity");
                return;
            }
            long bestStart = active.get(0), bestEnd = active.get(0), curStart = active.get(0);
            for (int i = 1; i <= active.size(); i++)
            {
                if (i < active.size() && active.get(i) - active.get(i - 1) < 2) continue;
                long curEnd = active.get(i - 1);
                if (curEnd - curStart > bestEnd - bestStart) { bestStart = curStart; bestEnd = curEnd; }
                if (i < active.size()) curStart = active.get(i);
            }
            long start = bestStart + 5, end = bestEnd - 1;
            if (end - start < 5) { start = bestStart; end = bestEnd + 1; }
            from = Instant.ofEpochSecond(start);
            to = Instant.ofEpochSecond(end);
        }
        final Instant f = from, t = to;
        java.util.function.Predicate<Instant> in = x -> fixed.any() ? fixed.contains(x) : !x.isBefore(f) && x.isBefore(t);

        long kapp = 0, ex = 0, nodeN = 0, paths = 0, nonFast = 0;
        double node = 0;
        Instant first = null, last = null;
        List<Double> client = new ArrayList<>(), pre = new ArrayList<>(), exe = new ArrayList<>(), deps = new ArrayList<>(), q = new ArrayList<>();
        Map<String, List<long[]>> workers = new HashMap<>(); // SharedPool worker -> [start, end, inAwait]
        try (RecordingFile rf = new RecordingFile(Path.of(file)))
        {
            while (rf.hasMoreEvents())
            {
                RecordedEvent e = rf.readEvent();
                String ev = e.getEventType().getName();
                if (ev.equals("profiler.WallClockSample"))
                {
                    String name = Threads.name(Threads.of(e));
                    if (!name.startsWith("SharedPool-Worker")) continue;
                    long s = e.getStartTime().toEpochMilli();
                    boolean await = Threads.hasFrame(e.getStackTrace(), "AccordResult.awaitAndGet");
                    workers.computeIfAbsent(name, k -> new ArrayList<>()).add(new long[]{ s, s + e.getDuration("timeSpan").toMillis(), await ? 1 : 0 });
                    continue;
                }
                if (!in.test(e.getStartTime())) continue;
                if (first == null || e.getStartTime().isBefore(first)) first = e.getStartTime();
                if (last == null || e.getStartTime().isAfter(last)) last = e.getStartTime();
                switch (ev)
                {
                    case "jdk.ExecutionSample":
                        if (Threads.name(Threads.of(e)).startsWith("AccordExecutor")) ex++;
                        break;
                    case "jdk.CPULoad":
                        node += e.getDouble("machineTotal"); nodeN++;
                        break;
                    case "profiler.Span":
                    {
                        String tag = e.getString("tag");
                        if (!tag.startsWith("DX ")) break;
                        double d = e.getDuration().toNanos() / 1e6;
                        if (tag.startsWith("DX K Applied")) kapp++;
                        else if (tag.startsWith("DX C Client")) client.add(d);
                        else if (tag.startsWith("DX C PreAccept")) pre.add(d);
                        else if (tag.startsWith("DX C Execute")) exe.add(d);
                        else if (tag.startsWith("DX Q ")) q.add(d);
                        else if (tag.startsWith("DX E Executing"))
                        {
                            Matcher m = DEPS.matcher(tag);
                            if (m.find())
                            {
                                deps.add(Double.parseDouble(m.group(2)));
                                paths++;
                                if (!m.group(1).equals("FAST")) nonFast++;
                            }
                        }
                        break;
                    }
                }
            }
        }
        if (first == null)
        {
            System.out.println(Path.of(file).getFileName() + ": no events in the window");
            return;
        }
        Instant wFrom = fixed.any() ? first : from, wTo = fixed.any() ? last : to;
        double secs = Math.max(1, Duration.between(wFrom, wTo).toMillis() / 1000.0);

        // transactions in flight: workers parked in awaitAndGet, on a 50ms grid over the window
        long lo = wFrom.toEpochMilli() / 50 * 50, hi = wTo.toEpochMilli();
        int nb = (int) Math.max(1, (hi - lo) / 50);
        int[] cnt = new int[nb];
        for (List<long[]> l : workers.values())
            for (long[] x : l)
            {
                if (x[2] == 0) continue;
                long end = Math.max(x[1], x[0] + 10);
                for (long g = Math.max(lo, (x[0] + 49) / 50 * 50); g <= Math.min(end, hi - 1); g += 50)
                    cnt[(int) ((g - lo) / 50)]++;
            }
        double sum = 0;
        int max = 0;
        for (int c : cnt) { sum += c; max = Math.max(max, c); }

        System.out.printf("%-41s %s-%s %6.0f %7.2f %5.0f %7.0f %6.0f %7.0f %6.0f %8.0f %5.1f %9.1f/%d%n",
                          Path.of(file).getFileName().toString().replaceAll("\\.jfr$", ""),
                          time(wFrom), time(wTo), kapp / secs, ex / 1000.0 / secs, nodeN == 0 ? Double.NaN : 100 * node / nodeN,
                          pct(client, .5), pct(pre, .5), pct(exe, .5), pct(deps, .5),
                          paths == 0 ? Double.NaN : 100.0 * nonFast / paths, pct(q, .99), sum / nb, max);
    }

    static String time(Instant t)
    {
        return LocalTime.ofInstant(t, ZoneOffset.UTC).withNano(0).toString();
    }

    static double pct(List<Double> v, double p)
    {
        if (v.isEmpty()) return Double.NaN;
        Collections.sort(v);
        return v.get(Math.min(v.size() - 1, (int) (v.size() * p)));
    }
}
