import java.time.*;
import java.util.*;

// LinkDelays [--bucket ms] [--excess ms] [--from HH:MM:SS[.sss]] [--to ...] [--skip s] <jfr>... :
// one-way message delay per directed node pair, from the DX message events of
// -Daccord.debug_distributed_execution=N (see Dx.Messages for how sends and receives are paired).
//   - per link: messages, min/p50/p90/p99/p999/max, and how many arrived more than --excess ms (default 50) later
//     than the link's median. The median should equal the configured or physical one-way latency; if it doesn't,
//     the hosts' clocks disagree.
//   - --bucket ms: per link, p50/max per bucket (by send time). A delay that climbs and then drains linearly is a
//     backlog on that connection (outbound queue or TCP); quantised spikes of a few RTTs look like retransmissions.
// The window options select messages by send time.
public class LinkDelays
{
    public static void main(String[] args) throws Exception
    {
        long bucketMs = 0, excessMs = 50; Dx.Window window = new Dx.Window(); List<String> files = new ArrayList<>();
        for (int i = 0; i < args.length; )
        {
            int n = window.accept(args, i);
            if (n > 0) { i += n; continue; }
            switch (args[i])
            {
                case "--bucket": bucketMs = Long.parseLong(args[i + 1]); i += 2; break;
                case "--excess": excessMs = Long.parseLong(args[i + 1]); i += 2; break;
                default: files.add(args[i++]);
            }
        }
        if (files.isEmpty())
        {
            System.err.println("usage: LinkDelays [--bucket ms] [--excess ms] " + Dx.Window.usage() + " <jfr>...");
            System.exit(2);
        }

        Dx.Messages messages = new Dx.Messages();
        Instant[] firstClient = { null };
        Dx.scan(files, ev -> {
            if (ev.tag().startsWith("C Client ") && (firstClient[0] == null || ev.start().isBefore(firstClient[0]))) firstClient[0] = ev.start();
            messages.accept(ev);
        });
        window.resolve(firstClient[0]);

        Map<String, List<Dx.Messages.Delay>> byLink = new TreeMap<>();
        Instant t0 = null;
        for (Dx.Messages.Delay d : messages.delays())
        {
            if (!window.contains(d.sent())) continue;
            byLink.computeIfAbsent(d.from() + "->" + d.to(), k -> new ArrayList<>()).add(d);
            if (t0 == null || d.sent().isBefore(t0)) t0 = d.sent();
        }
        System.out.printf("node ids %s; %s; %d message events unpaired%n", messages.names(), window.describe(), messages.unpaired);
        if (byLink.isEmpty()) { System.out.println("no paired messages (are DX spans enabled, with M events?)"); return; }

        final long excessNanos = excessMs * 1_000_000;
        System.out.printf("%-24s %8s %8s %8s %8s %8s %8s %9s  %s%n", "link", "n", "min", "p50", "p90", "p99", "p999", "max", ">p50+" + excessMs + "ms  (ms)");
        for (var en : byLink.entrySet())
        {
            long[] d = en.getValue().stream().mapToLong(Dx.Messages.Delay::nanos).sorted().toArray();
            long p50 = Dx.percentile(d, 0.5), over = Arrays.stream(d).filter(x -> x > p50 + excessNanos).count();
            System.out.printf("%-24s %8d %8.1f %8.1f %8.1f %8.1f %8.1f %9.1f  %d (%.2f%%)%n", en.getKey(), d.length, Dx.ms(d[0]), Dx.ms(p50),
                              Dx.ms(Dx.percentile(d, 0.9)), Dx.ms(Dx.percentile(d, 0.99)), Dx.ms(Dx.percentile(d, 0.999)), Dx.ms(d[d.length - 1]), over, 100.0 * over / d.length);
        }

        if (bucketMs <= 0) return;
        final Instant zero = t0;
        System.out.printf("%nper %dms bucket from %s UTC (by send time): p50/max one-way ms%n", bucketMs, Dx.utc(zero));
        for (var en : byLink.entrySet())
        {
            TreeMap<Long, List<Long>> buckets = new TreeMap<>();
            for (Dx.Messages.Delay d : en.getValue())
                buckets.computeIfAbsent(Duration.between(zero, d.sent()).toMillis() / bucketMs, k -> new ArrayList<>()).add(d.nanos());
            StringBuilder sb = new StringBuilder(String.format("%-24s", en.getKey()));
            for (long b = buckets.firstKey(); b <= buckets.lastKey(); b++)
            {
                List<Long> l = buckets.get(b);
                if (l == null) { sb.append("       -     "); continue; }
                long[] d = l.stream().mapToLong(Long::longValue).sorted().toArray();
                sb.append(String.format(" %5.0f/%-6.0f", Dx.ms(Dx.percentile(d, 0.5)), Dx.ms(d[d.length - 1])));
            }
            System.out.println(sb);
        }
    }
}
