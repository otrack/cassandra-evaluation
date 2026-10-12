import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.regex.*;

// TxnTrace [--slowest N | --txn <substring> | --key <substring>] [--min-ms M] [--depth D]
//          [--from HH:MM:SS[.sss]] [--to HH:MM:SS[.sss]] [--skip <seconds>] <jfr>... :
// joins the "DX ..." spans emitted with -Daccord.debug_distributed_execution[_keys]=N across the recordings of all
// nodes.
//   default / --slowest N : the N slowest coordinated transactions (by their "C Client" span): one timeline each,
//                           relative to the start of the Client span, then their wait chains (below)
//   --txn <substring>     : the same for the transactions whose TxnId contains <substring>
//   --key <substring>     : every K event of the keys whose name contains <substring>, in time order
//   (with key events only, i.e. no Client spans: the N longest waits from K Stable to the key releasing the waiter)
// A wait chain follows "K Unblock <waiter> k<key> s<store> by <trigger>" events: for each replica (node, store) the
// last Unblock of the waiter is the one that released it; its trigger is then followed on the same replica, up to
// --depth (default 4) steps.
// --from/--to (UTC) and --skip (seconds after the first Client span) restrict --slowest to transactions whose Client
// span started in the window, --key to key events in it, and the key-only waits to those released in it; e.g.
// --skip 30 to leave out client start-up. TxnLatency gives the overview (and the TxnIds) these timelines drill into.
// Node names come from the file names (..._<Node>.jfr) or from in-JVM dtest thread prefixes (nodeN_). Cross-node
// offsets are only as good as the hosts' clock sync.
public class TxnTrace
{
    record Ev(Instant start, long durNanos, String node, String thread, String tag) {}
    static final Pattern TXN = Dx.TXN;
    static final Pattern UNBLOCK = Pattern.compile("^K Unblock (\\[[^\\]]*\\]) k(\\S+) s(\\d+) by (\\S+)$");

    static final Map<String, List<Ev>> byTxn = new HashMap<>();
    static final List<Ev> keyEvents = new ArrayList<>();
    static final Map<String, Ev> client = new HashMap<>();

    public static void main(String[] args) throws Exception
    {
        int slowest = 5, depth = 4; String only = null, key = null; long minMs = 0; List<String> files = new ArrayList<>();
        Dx.Window window = new Dx.Window();
        for (int i = 0; i < args.length; i++)
        {
            int consumed = window.accept(args, i);
            if (consumed > 0) { i += consumed - 1; continue; }
            switch (args[i])
            {
                case "--slowest": slowest = Integer.parseInt(args[++i]); break;
                case "--txn": only = args[++i]; break;
                case "--key": key = args[++i]; break;
                case "--min-ms": minMs = Long.parseLong(args[++i]); break;
                case "--depth": depth = Integer.parseInt(args[++i]); break;
                default: files.add(args[i]);
            }
        }

        Dx.scan(files, TxnTrace::add);
        for (List<Ev> l : byTxn.values()) l.sort(Comparator.comparing(Ev::start));
        keyEvents.sort(Comparator.comparing(Ev::start));
        window.resolve(client.values().stream().map(Ev::start).min(Comparator.naturalOrder())
                             .orElse(keyEvents.isEmpty() ? null : keyEvents.get(0).start()));
        System.out.printf("%d traced transactions (%d with a Client span), %d key events, across %d recordings; %s%n", byTxn.size(), client.size(), keyEvents.size(), files.size(), window.describe());

        if (key != null)
        {
            Instant zero = null;
            for (Ev ev : keyEvents)
            {
                if (!window.contains(ev.start())) continue;
                if (!ev.tag().contains(" k" + key) && !ev.tag().matches(".* k\\S*" + Pattern.quote(key) + "\\S* .*")) continue;
                if (zero == null) zero = ev.start();
                System.out.printf("  %+10.2fms  %-10s %s%n", Duration.between(zero, ev.start()).toNanos() / 1e6, ev.node(), ev.tag());
            }
            return;
        }

        if (only == null && client.isEmpty())
        {
            // key events only (accord.debug_distributed_execution_keys without transaction sampling): the longest
            // waits between a transaction becoming Stable on a replica and its key releasing it, with their chains
            record Wait(String txn, String replica, Ev stable, Ev released) {}
            List<Wait> waits = new ArrayList<>();
            for (var en : byTxn.entrySet())
            {
                Map<String, Ev> stable = new HashMap<>(), released = new HashMap<>();
                for (Ev ev : en.getValue())
                {
                    Matcher m = UNBLOCK.matcher(ev.tag());
                    if (m.matches() && m.group(1).equals(en.getKey())) released.put(ev.node() + " s" + m.group(3), ev);
                    else if (ev.tag().startsWith("K Stable " + en.getKey()))
                        stable.putIfAbsent(ev.node() + " s" + ev.tag().replaceAll("^.* s(\\d+).*$", "$1"), ev);
                }
                for (var r : released.entrySet())
                    if (stable.containsKey(r.getKey()) && window.contains(r.getValue().start())) waits.add(new Wait(en.getKey(), r.getKey(), stable.get(r.getKey()), r.getValue()));
            }
            waits.sort((x, y) -> Duration.between(y.stable().start(), y.released().start()).compareTo(Duration.between(x.stable().start(), x.released().start())));
            System.out.printf("no Client spans: the %d longest Stable -> released waits instead%n", Math.min(slowest, waits.size()));
            for (Wait w : waits.subList(0, Math.min(slowest, waits.size())))
            {
                System.out.printf("%n== %s on %s: waited %.2fms after K Stable%n", w.txn(), w.replica(), Duration.between(w.stable().start(), w.released().start()).toNanos() / 1e6);
                chain(w.stable().start(), w.txn(), w.replica(), Instant.MAX, depth, "     ");
            }
            return;
        }

        List<String> chosen = new ArrayList<>();
        if (only != null)
        {
            for (String t : byTxn.keySet()) if (t.contains(only)) chosen.add(t);
        }
        else
        {
            final long min = minMs;
            client.entrySet().stream()
                  .filter(en -> en.getValue().durNanos() >= min * 1_000_000)
                  .filter(en -> window.contains(en.getValue().start()))
                  .sorted((a, b) -> Long.compare(b.getValue().durNanos(), a.getValue().durNanos()))
                  .limit(slowest).forEach(en -> chosen.add(en.getKey()));
        }

        for (String txn : chosen)
        {
            List<Ev> evs = byTxn.get(txn);
            Ev c = client.get(txn);
            Instant zero = c != null ? c.start() : evs.get(0).start();
            System.out.printf("%n== %s  %s%n", txn, c == null ? "(no Client span: not coordinated in these recordings)" : String.format("client %.1fms on %s", c.durNanos() / 1e6, c.node()));
            for (Ev ev : evs)
            {
                String dur = ev.durNanos() > 0 ? String.format("%8.2fms", ev.durNanos() / 1e6) : "         ";
                System.out.printf("  %+9.2fms %s  %-10s %-30s %s%n", at(zero, ev), dur, ev.node(), ev.thread().replaceAll("^node\\d+_", ""), ev.tag().replace(txn + " ", "").replace(txn, "*"));
            }
            System.out.println("  -- wait chains (per replica: the Unblock that released it, then its trigger's, ...)");
            Set<String> replicas = new TreeSet<>();
            for (Ev ev : evs)
            {
                Matcher m = UNBLOCK.matcher(ev.tag());
                if (m.matches() && m.group(1).equals(txn)) replicas.add(ev.node() + " s" + m.group(3));
            }
            if (replicas.isEmpty()) System.out.println("     (no Unblock events: enable -Daccord.debug_distributed_execution_keys, or the transaction never waited on a key)");
            for (String replica : replicas)
                chain(zero, txn, replica, Instant.MAX, depth, "     ");
        }
    }

    // the last Unblock of waiter on this replica (no later than 'before'), then recurse into its trigger
    static void chain(Instant zero, String waiter, String replica, Instant before, int depth, String indent)
    {
        Ev last = null; Matcher lastM = null;
        for (Ev ev : byTxn.getOrDefault(waiter, List.of()))
        {
            if (ev.start().isAfter(before)) break;
            Matcher m = UNBLOCK.matcher(ev.tag());
            if (m.matches() && m.group(1).equals(waiter) && (ev.node() + " s" + m.group(3)).equals(replica)) { last = ev; lastM = m; }
        }
        if (last == null) return;
        String by = lastM.group(4);
        Ev stable = null;
        for (Ev ev : byTxn.get(waiter))
            if (ev.node().equals(last.node()) && ev.tag().startsWith("K Stable " + waiter) && ev.tag().contains(" s" + lastM.group(3)) && !ev.start().isAfter(last.start())) stable = ev;
        boolean self = by.equals(waiter);
        // the trigger's own update of this key that caused the release (e.g. "Applied"), if recorded
        String cause = "";
        if (!self)
        {
            for (Ev ev : byTxn.getOrDefault(by, List.of()))
            {
                if (ev.start().isAfter(last.start())) break;
                if (ev.node().equals(last.node()) && ev.tag().startsWith("K ") && ev.tag().contains(" " + by + " k" + lastM.group(2) + " s" + lastM.group(3))
                    && !ev.tag().startsWith("K Unblock") && !ev.tag().startsWith("K BlockedOn"))
                    cause = String.format(" (its %s at %+.2fms)", ev.tag().split(" ")[1], at(zero, ev));
            }
        }
        System.out.printf("%s%s %s k%s: released %+.2fms%s by %s%s%n", indent, replica, waiter, lastM.group(2), at(zero, last),
                          stable == null ? "" : String.format(" (%.2fms after its K Stable here)", Duration.between(stable.start(), last.start()).toNanos() / 1e6),
                          self ? "itself (nothing earlier held it on this key)" : by, cause);
        if (depth > 1 && !self && by.startsWith("["))
            chain(zero, by, replica, last.start(), depth - 1, indent + "  ");
    }

    static double at(Instant zero, Ev ev)
    {
        return Duration.between(zero, ev.start()).toNanos() / 1e6;
    }

    static void add(Dx.Ev dx)
    {
        Ev ev = new Ev(dx.start(), dx.durNanos(), dx.node(), dx.thread(), dx.tag());
        if (ev.tag().startsWith("K ")) keyEvents.add(ev);
        // attach the event to every transaction it mentions (e.g. both waiter and trigger of an Unblock)
        Matcher m = TXN.matcher(ev.tag());
        Set<String> seen = new HashSet<>();
        while (m.find())
            if (seen.add(m.group())) byTxn.computeIfAbsent(m.group(), k -> new ArrayList<>()).add(ev);
        if (ev.tag().startsWith("C Client") && !seen.isEmpty())
            client.put(seen.iterator().next(), ev);
    }
}
