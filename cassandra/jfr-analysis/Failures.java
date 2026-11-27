import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// Failures [--from HH:MM:SS[.sss]] [--to ...] [--top N] <jfr>... : why client transactions failed, and what recovery
// did, from AccordFailureDiagnostics' events (-Dcassandra.jfr.diagnostic_events=true; diagnostics.jfc enables them):
//   failures      cassandra.accord.TxnFailure: by kind (Timeout, Preempted, Exhausted, ...), per second, how long they
//                 took and how far past their deadline, where they were raised (origin), and which of this node's
//                 coordinations of the txn were still registered when it failed (by kind)
//   coordinations cassandra.accord.Coordination: recovery, invalidation, fetch and progress log coordinations, by kind:
//                 count, completed, duration percentiles, per second; the txns coordinated most often; and the
//                 commonest last traced messages (normally the tracker summary), numbers elided
//   blocked       cassandra.accord.TxnBlockedWalk / TxnBlocked: for the sampled failures, what they waited on: walks by
//                 outcome, txns visited per walk; per depth the save statuses of the txns visited; the "leaves" (txns
//                 visited that wait on nothing, or at the walk's depth limit) by save status and whether the store's
//                 progress log was working on them; and the txns blocking the most failed txns
public class Failures
{
    public static void main(String[] a) throws Exception
    {
        LocalTime from = null, to = null;
        int top = 10;
        List<String> files = new ArrayList<>();
        for (int i = 0 ; i < a.length ; ++i)
        {
            switch (a[i])
            {
                case "--from": from = LocalTime.parse(a[++i]); break;
                case "--to": to = LocalTime.parse(a[++i]); break;
                case "--top": top = Integer.parseInt(a[++i]); break;
                default: files.add(a[i]);
            }
        }
        if (files.isEmpty())
        {
            System.err.println("usage: Failures [--from HH:MM:SS[.sss]] [--to ...] [--top N] <jfr>...");
            System.exit(1);
        }

        for (String file : files)
        {
            List<RecordedEvent> failures = new ArrayList<>(), coordinations = new ArrayList<>(), walks = new ArrayList<>(), blocked = new ArrayList<>();
            try (RecordingFile rf = new RecordingFile(Path.of(file)))
            {
                while (rf.hasMoreEvents())
                {
                    RecordedEvent e = rf.readEvent();
                    String name = e.getEventType().getName();
                    List<RecordedEvent> into;
                    switch (name)
                    {
                        case "cassandra.accord.TxnFailure": into = failures; break;
                        case "cassandra.accord.Coordination": into = coordinations; break;
                        case "cassandra.accord.TxnBlockedWalk": into = walks; break;
                        case "cassandra.accord.TxnBlocked": into = blocked; break;
                        default: continue;
                    }
                    LocalTime t = LocalTime.ofInstant(e.getStartTime(), ZoneOffset.UTC);
                    if ((from != null && t.isBefore(from)) || (to != null && t.isAfter(to)))
                        continue;
                    into.add(e);
                }
            }

            System.out.println(Path.of(file).getFileName());
            if (failures.isEmpty() && coordinations.isEmpty() && walks.isEmpty())
            {
                System.out.println("  no cassandra.accord.TxnFailure/Coordination/TxnBlocked events (older build, events disabled, or nothing failed)");
                continue;
            }
            printFailures(failures, top);
            printCoordinations(coordinations, top);
            printBlocked(walks, blocked, top);
        }
    }

    static void printFailures(List<RecordedEvent> failures, int top)
    {
        System.out.printf("  failures: %d%n", failures.size());
        if (failures.isEmpty())
            return;
        Map<String, Integer> kinds = new TreeMap<>(), origins = new HashMap<>(), still = new HashMap<>();
        TreeMap<String, Map<String, Integer>> perSecond = new TreeMap<>();
        Map<String, List<Long>> elapsed = new TreeMap<>(), past = new TreeMap<>();
        int walked = 0;
        for (RecordedEvent e : failures)
        {
            String kind = e.getString("kind");
            kinds.merge(kind, 1, Integer::sum);
            origins.merge(kind + " @ " + e.getString("origin"), 1, Integer::sum);
            perSecond.computeIfAbsent(second(e), k -> new TreeMap<>()).merge(kind, 1, Integer::sum);
            elapsed.computeIfAbsent(kind, k -> new ArrayList<>()).add(e.getDuration("elapsed").toMillis());
            past.computeIfAbsent(kind, k -> new ArrayList<>()).add(e.getDuration("pastDeadline").toMillis());
            String c = e.getString("coordinations");
            if (c == null || c.isEmpty()) still.merge("(none)", 1, Integer::sum);
            else
            {
                Set<String> seen = new TreeSet<>();
                for (String part : c.split(" \\| "))
                    seen.add(part.trim().split(" ")[0]);
                still.merge(String.join("+", seen), 1, Integer::sum);
            }
            if (e.getBoolean("walked")) ++walked;
        }
        System.out.println("    by kind        " + kinds + String.format("  (%d sampled for a blocked walk)", walked));
        for (String kind : elapsed.keySet())
            System.out.printf("    %-14s elapsed ms %s; past deadline ms %s%n", kind, percentiles(elapsed.get(kind)), percentiles(past.get(kind)));
        System.out.println("    per second     " + perSecondLine(perSecond));
        System.out.println("    raised at      " + topN(origins, top));
        System.out.println("    still running  " + topN(still, top) + "  (this node's coordinations of the txn when it failed)");
    }

    static void printCoordinations(List<RecordedEvent> coordinations, int top)
    {
        System.out.printf("  coordinations (recovery, invalidation, fetch, progress log): %d%n", coordinations.size());
        if (coordinations.isEmpty())
            return;
        Map<String, List<Long>> durations = new TreeMap<>();
        Map<String, Integer> done = new TreeMap<>(), perTxn = new HashMap<>(), traces = new HashMap<>();
        TreeMap<String, Map<String, Integer>> perSecond = new TreeMap<>();
        for (RecordedEvent e : coordinations)
        {
            String kind = e.getString("kind");
            durations.computeIfAbsent(kind, k -> new ArrayList<>()).add(e.getDuration().toMillis());
            if (e.getBoolean("done")) done.merge(kind, 1, Integer::sum);
            if (!kind.endsWith("Progress")) perTxn.merge(e.getString("txnId"), 1, Integer::sum);
            perSecond.computeIfAbsent(second(e), k -> new TreeMap<>()).merge(kind, 1, Integer::sum);
            String last = e.getString("lastTraces");
            if (last != null && !last.isEmpty())
                traces.merge(kind + ": " + elide(last), 1, Integer::sum);
        }
        for (String kind : durations.keySet())
            System.out.printf("    %-18s n=%-6d done=%-6d duration ms %s%n", kind, durations.get(kind).size(), done.getOrDefault(kind, 0), percentiles(durations.get(kind)));
        System.out.println("    per second         " + perSecondLine(perSecond));
        System.out.println("    most coordinated   " + topN(perTxn, top) + "  (txns, excluding progress log activity)");
        List<Map.Entry<String, Integer>> t = new ArrayList<>(traces.entrySet());
        t.sort((x, y) -> y.getValue() - x.getValue());
        System.out.println("    commonest last traces:");
        for (int i = 0 ; i < Math.min(top, t.size()) ; ++i)
            System.out.printf("      %6d  %s%n", t.get(i).getValue(), t.get(i).getKey().length() > 220 ? t.get(i).getKey().substring(0, 220) + "..." : t.get(i).getKey());
    }

    static void printBlocked(List<RecordedEvent> walks, List<RecordedEvent> blocked, int top)
    {
        System.out.printf("  blocked walks: %d%n", walks.size());
        if (walks.isEmpty())
            return;
        Map<String, Integer> outcomes = new TreeMap<>();
        List<Long> visited = new ArrayList<>(), durations = new ArrayList<>();
        for (RecordedEvent e : walks)
        {
            outcomes.merge(e.getString("outcome"), 1, Integer::sum);
            visited.add((long) e.getInt("visited"));
            durations.add(e.getDuration().toMillis());
        }
        System.out.println("    outcomes       " + outcomes);
        System.out.println("    visited/walk   " + percentiles(visited) + "; walk ms " + percentiles(durations));

        TreeMap<Integer, Map<String, Integer>> byDepth = new TreeMap<>();
        Map<String, Integer> leaves = new HashMap<>(), blockers = new HashMap<>();
        int maxDepth = 0;
        for (RecordedEvent e : blocked) maxDepth = Math.max(maxDepth, e.getInt("depth"));
        for (RecordedEvent e : blocked)
        {
            int depth = e.getInt("depth");
            String status = e.getString("saveStatus");
            if (status == null || status.isEmpty()) status = e.getString("txnId").isEmpty() ? "(key not blocked)" : "(none)";
            byDepth.computeIfAbsent(depth, k -> new TreeMap<>()).merge(status, 1, Integer::sum);
            boolean waits = e.getInt("blockedByCount") > 0 || e.getInt("blockedByKeyCount") > 0;
            if (!waits || depth == maxDepth)
            {
                String progress = e.getBoolean("progressHomeActive") ? (e.getBoolean("progressWaitingActive") ? "home+waiting" : "home")
                                                                       : (e.getBoolean("progressWaitingActive") ? "waiting" : "");
                leaves.merge(status + (waits ? " (at depth limit)" : "") + " progress=" + (progress.isEmpty() ? "none" : progress), 1, Integer::sum);
            }
            if (depth > 0 && !e.getString("txnId").isEmpty())
                blockers.merge(e.getString("txnId") + " " + status, 1, Integer::sum);
        }
        for (Map.Entry<Integer, Map<String, Integer>> d : byDepth.entrySet())
            System.out.println("    depth " + d.getKey() + "        " + topN(d.getValue(), top));
        System.out.println("    leaves         " + topN(leaves, top));
        System.out.println("    top blockers   " + topN(blockers, top) + "  (txn status: walks it appeared in below the root)");
    }

    static String second(RecordedEvent e)
    {
        return LocalTime.ofInstant(e.getStartTime(), ZoneOffset.UTC).withNano(0).toString();
    }

    static String perSecondLine(TreeMap<String, Map<String, Integer>> perSecond)
    {
        StringBuilder b = new StringBuilder();
        for (Map.Entry<String, Map<String, Integer>> s : perSecond.entrySet())
        {
            int n = 0;
            for (int v : s.getValue().values()) n += v;
            b.append(s.getKey().substring(Math.max(0, s.getKey().length() - 5))).append('=').append(n).append(' ');
        }
        return b.toString().trim();
    }

    static String topN(Map<String, Integer> m, int n)
    {
        List<Map.Entry<String, Integer>> l = new ArrayList<>(m.entrySet());
        l.sort((x, y) -> y.getValue() - x.getValue());
        StringBuilder b = new StringBuilder();
        for (int i = 0 ; i < Math.min(n, l.size()) ; ++i)
            b.append(l.get(i).getKey()).append('=').append(l.get(i).getValue()).append("  ");
        if (l.size() > n) b.append("(+").append(l.size() - n).append(" more)");
        return b.toString().trim();
    }

    static String percentiles(List<Long> values)
    {
        if (values.isEmpty()) return "-";
        List<Long> s = new ArrayList<>(values);
        Collections.sort(s);
        return String.format("p50=%d p90=%d p99=%d max=%d", s.get(s.size() / 2), s.get((int) (s.size() * 0.9)), s.get(Math.min(s.size() - 1, (int) (s.size() * 0.99))), s.get(s.size() - 1));
    }

    // group similar messages: numbers (ids, epochs, counts) elided
    static String elide(String s)
    {
        return s.replaceAll("\\[[0-9]+,[0-9]+,[0-9]+\\([A-Z]+\\),[0-9]+\\]", "<txn>").replaceAll("[0-9]+", "N");
    }
}
