import java.time.*;
import java.util.*;
import java.util.regex.*;

// TxnLatency [--slow-ms M] [--excess ms] [--top N] [--keys N] [--bucket ms] [--from ..] [--to ..] [--skip s] <jfr>... :
// where the tail is, from the "C Client" spans of -Daccord.debug_distributed_execution=N (the coordinator's view of
// each traced transaction, from AccordService.coordinateAsync to its result). Transactions are selected by the start
// of their Client span.
//   - per coordinator and kind (KW key write, KE ephemeral read, ...): percentiles and the slow ones: at least --slow-ms,
//     or by default at least 2.5x that coordinator and kind's median
//   - "msg": of the slow ones, how many had one of their own messages (received before the client span ended) arrive
//     at least --excess ms (default 50) later than its link's median: delayed by messaging rather than by waiting.
//     Waiting on another transaction that was itself delayed by messaging is not counted.
//   - by kind and execution path (FAST/MEDIUM/SLOW/EPHEMERAL, from "E Executing") and by key (from the transaction's
//     own K events; needs the transaction or its key to be sampled)
//   - the --top N (default 10) slowest, with their TxnIds, for TxnTrace --txn <hlc>
//   - --bucket ms: per coordinator and kind, count and p99/max per bucket
public class TxnLatency
{
    record Client(String txn, String node, Instant start, long nanos) {}

    public static void main(String[] args) throws Exception
    {
        long slowMs = -1, excessMs = 50, bucketMs = 0; int top = 10, keys = 10;
        Dx.Window window = new Dx.Window(); List<String> files = new ArrayList<>();
        for (int i = 0; i < args.length; )
        {
            int n = window.accept(args, i);
            if (n > 0) { i += n; continue; }
            switch (args[i])
            {
                case "--slow-ms": slowMs = Long.parseLong(args[i + 1]); i += 2; break;
                case "--excess": excessMs = Long.parseLong(args[i + 1]); i += 2; break;
                case "--top": top = Integer.parseInt(args[i + 1]); i += 2; break;
                case "--keys": keys = Integer.parseInt(args[i + 1]); i += 2; break;
                case "--bucket": bucketMs = Long.parseLong(args[i + 1]); i += 2; break;
                default: files.add(args[i++]);
            }
        }
        if (files.isEmpty())
        {
            System.err.println("usage: TxnLatency [--slow-ms M] [--excess ms] [--top N] [--keys N] [--bucket ms] " + Dx.Window.usage() + " <jfr>...");
            System.exit(2);
        }

        Map<String, Client> clients = new HashMap<>();
        Map<String, String> path = new HashMap<>(), keyOf = new HashMap<>();
        Dx.Messages messages = new Dx.Messages();
        Dx.scan(files, ev -> {
            Matcher m;
            if ((m = Dx.CLIENT.matcher(ev.tag())).find()) clients.put(m.group(1), new Client(m.group(1), ev.node(), ev.start(), ev.durNanos()));
            else if ((m = Dx.EXECUTING.matcher(ev.tag())).find()) path.put(m.group(1), m.group(2));
            else if ((m = Dx.KEY_UPDATE.matcher(ev.tag())).matches()) keyOf.putIfAbsent(m.group(2), m.group(3));
            messages.accept(ev);
        });
        window.resolve(clients.values().stream().map(Client::start).min(Comparator.naturalOrder()).orElse(null));

        // each transaction's worst message: the largest excess over its link's median, among its messages received
        // before its client span ended
        List<Dx.Messages.Delay> delays = messages.delays();
        Map<String, List<Long>> byLink = new HashMap<>();
        for (Dx.Messages.Delay d : delays) byLink.computeIfAbsent(d.from() + "->" + d.to(), k -> new ArrayList<>()).add(d.nanos());
        Map<String, Long> median = new HashMap<>();
        byLink.forEach((l, v) -> { Collections.sort(v); median.put(l, v.get(v.size() / 2)); });
        Map<String, long[]> worstExcess = new HashMap<>(); Map<String, String> worstLink = new HashMap<>();
        for (Dx.Messages.Delay d : delays)
        {
            Client c = clients.get(d.txn());
            if (c == null) continue;
            if (d.sent().plusNanos(d.nanos()).isAfter(c.start().plusNanos(c.nanos()))) continue;
            String link = d.from() + "->" + d.to();
            long excess = d.nanos() - median.get(link);
            long[] w = worstExcess.computeIfAbsent(d.txn(), k -> new long[]{ Long.MIN_VALUE });
            if (excess > w[0]) { w[0] = excess; worstLink.put(d.txn(), d.type() + " " + link); }
        }

        List<Client> selected = new ArrayList<>();
        for (Client c : clients.values()) if (window.contains(c.start())) selected.add(c);
        System.out.printf("%d traced transactions with a Client span, %d selected (%s), %d recordings%n", clients.size(), selected.size(), window.describe(), files.size());
        if (selected.isEmpty()) return;

        Map<String, List<Client>> groups = new TreeMap<>();
        for (Client c : selected) groups.computeIfAbsent(c.node() + " " + Dx.kind(c.txn()), k -> new ArrayList<>()).add(c);
        Map<String, Long> threshold = new HashMap<>();
        final long excessNanos = excessMs * 1_000_000;
        Set<String> slow = new HashSet<>();

        System.out.printf("%n%-16s %7s %8s %8s %8s %8s %8s %8s  %5s %6s %7s   (ms)%n", "coord kind", "n", "p50", "p90", "p99", "p99.9", "max", "slow>=", "slow", "slow%", "msg%");
        for (var en : groups.entrySet())
        {
            long[] d = en.getValue().stream().mapToLong(Client::nanos).sorted().toArray();
            long thr = slowMs >= 0 ? slowMs * 1_000_000 : (long) (2.5 * Dx.percentile(d, 0.5));
            threshold.put(en.getKey(), thr);
            int nSlow = 0, nMsg = 0;
            for (Client c : en.getValue())
            {
                if (c.nanos() < thr) continue;
                nSlow++; slow.add(c.txn());
                long[] w = worstExcess.get(c.txn());
                if (w != null && w[0] >= excessNanos) nMsg++;
            }
            System.out.printf("%-16s %7d %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f  %5d %5.1f%% %6.1f%%%n", en.getKey(), d.length,
                              Dx.ms(Dx.percentile(d, 0.5)), Dx.ms(Dx.percentile(d, 0.9)), Dx.ms(Dx.percentile(d, 0.99)), Dx.ms(Dx.percentile(d, 0.999)),
                              Dx.ms(d[d.length - 1]), Dx.ms(thr), nSlow, 100.0 * nSlow / d.length, nSlow == 0 ? 0 : 100.0 * nMsg / nSlow);
        }
        System.out.printf("  slow: >= %s; msg: share of the slow ones with a message >= %dms over its link's median%n",
                          slowMs >= 0 ? slowMs + "ms" : "2.5x the group's p50", excessMs);

        Map<String, int[]> byPath = new TreeMap<>();
        for (Client c : selected)
        {
            int[] v = byPath.computeIfAbsent(Dx.kind(c.txn()) + " " + path.getOrDefault(c.txn(), "(not executed)"), k -> new int[2]);
            v[0]++; if (slow.contains(c.txn())) v[1]++;
        }
        System.out.printf("%n%-28s %8s %7s %7s%n", "kind and path", "n", "slow", "slow%");
        byPath.forEach((p, v) -> System.out.printf("%-28s %8d %7d %6.2f%%%n", p, v[0], v[1], 100.0 * v[1] / v[0]));

        if (keys > 0)
        {
            Map<String, int[]> byKey = new HashMap<>();
            for (Client c : selected)
            {
                int[] v = byKey.computeIfAbsent(keyOf.getOrDefault(c.txn(), "(no K events)"), k -> new int[2]);
                v[0]++; if (slow.contains(c.txn())) v[1]++;
            }
            List<Map.Entry<String, int[]>> l = new ArrayList<>(byKey.entrySet());
            l.sort((x, y) -> y.getValue()[1] != x.getValue()[1] ? y.getValue()[1] - x.getValue()[1] : y.getValue()[0] - x.getValue()[0]);
            System.out.printf("%nkeys by slow transactions (%d keys, %d slow):%n", byKey.size(), slow.size());
            System.out.printf("  %-44s %8s %7s %7s %8s%n", "key", "n", "slow", "slow%", "cum%slow");
            int cum = 0;
            for (var en : l.subList(0, Math.min(keys, l.size())))
            {
                cum += en.getValue()[1];
                System.out.printf("  %-44s %8d %7d %6.1f%% %7.1f%%%n", en.getKey(), en.getValue()[0], en.getValue()[1],
                                  100.0 * en.getValue()[1] / en.getValue()[0], slow.isEmpty() ? 0 : 100.0 * cum / slow.size());
            }
        }

        if (top > 0)
        {
            List<Client> sorted = new ArrayList<>(selected);
            sorted.sort((x, y) -> Long.compare(y.nanos(), x.nanos()));
            System.out.printf("%nslowest %d (TxnTrace --txn <hlc> for the timeline):%n", Math.min(top, sorted.size()));
            System.out.printf("  %9s  %-10s %-12s %-34s %-10s %-26s %s%n", "ms", "coord", "start UTC", "txn", "path", "key", "worst message (excess ms)");
            for (Client c : sorted.subList(0, Math.min(top, sorted.size())))
            {
                long[] w = worstExcess.get(c.txn());
                System.out.printf("  %9.1f  %-10s %-12s %-34s %-10s %-26s %s%n", Dx.ms(c.nanos()), c.node(), Dx.utc(c.start()), c.txn(),
                                  path.getOrDefault(c.txn(), "-"), keyOf.getOrDefault(c.txn(), "-"),
                                  w == null ? "-" : String.format("%s %+.1f", worstLink.get(c.txn()), Dx.ms(w[0])));
            }
        }

        if (bucketMs > 0)
        {
            Instant zero = selected.stream().map(Client::start).min(Comparator.naturalOrder()).get();
            System.out.printf("%nper %dms bucket from %s UTC: count p99/max (ms)%n", bucketMs, Dx.utc(zero));
            for (var en : groups.entrySet())
            {
                TreeMap<Long, List<Long>> b = new TreeMap<>();
                for (Client c : en.getValue()) b.computeIfAbsent(Duration.between(zero, c.start()).toMillis() / bucketMs, k -> new ArrayList<>()).add(c.nanos());
                StringBuilder sb = new StringBuilder(String.format("%-16s", en.getKey()));
                for (var bb : b.entrySet())
                {
                    long[] d = bb.getValue().stream().mapToLong(Long::longValue).sorted().toArray();
                    sb.append(String.format(" %3d:%5d %4.0f/%-5.0f", bb.getKey(), d.length, Dx.ms(Dx.percentile(d, 0.99)), Dx.ms(d[d.length - 1])));
                }
                System.out.println(sb);
            }
        }
    }
}
