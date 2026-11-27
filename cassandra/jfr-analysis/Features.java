import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// Features [--from HH:MM:SS[.sss]] [--to ...] [--stores] <jfr>... : whether the executor features the in-flight patches
// add are actually engaging, from the cumulative counters JfrDiagnostics exports (builds with these fields; older
// captures print "n/a"). Over the window (default: the whole recording), the change in each counter, per node:
//
//  deadlines (cassandra.accord.Executor, summed over executors): deadline-ordered delivery of contended PreAccepts
//    decisions   how each PreAccept over keys was decided (PreAcceptDeadlines.Counter; exactly one each): wrongGroup, presetup,
//                notPreAccept, noDeferral, notKeyTxn, positionMismatch, noCfk, notContended, noPenalty, alreadyDue,
//                assigned; as counts and shares
//    cfk         what the contention check found on each CommandsForKey it examined: unsafe (skipped), unloaded or
//                shrunk (so only its queue was consulted), recent (contended by a witnessed txn), queued (contended by
//                a queued task), quiet
//    fate        of those assigned: deferred, expired before they could be deferred, released by their HLC deadline
//                or by the receipt bound; the mean planned wait (deadline - now) and mean time deferred (creation to
//                release); the penalty (us) and most deferred at once over the window
//
//  stores (cassandra.accord.Store, summed over stores; --stores also per store): hand-offs, continuation, batching
//    tasks       runs (heads and continued tasks) + batched (followers and inlined), and per hand-off
//    continue    continued tasks, and why runs did not continue: nothing waiting, the executor lock busy, the
//                continue limit reached with work waiting, or nothing to prepare once locked
//    batch       batches, batched (followers + inlined), inlined, parked operations; attempts whose head could not
//                lead a batch, that found nothing queued behind the head, or candidates none of which could follow
//                (the last two include after-run attempts, which may form a batch only to inline consequences);
//                candidates rejected for the budget or as not batchable followers; after-run attempts with the lock busy
//    (newer builds) batch sizes; why formation took no more followers; where followers came from and the candidates
//                considered per attempt; reverted followers by why their batch stopped; parked operations by kind; the
//                reasons heads could not lead, no candidate could follow, and consequences could not be inlined (with
//                the top kind/REASON of the latter); and the lock time spent forming and completing batches, and
//                completing tasks that led none
//  lock (cassandra.accord.Executor, with accord.debug.execution): per executor, the wall and CPU time its lock was held
//                per second, per acquisition and per task of its stores, and acquisitions per task and per hand-off
public class Features
{
    static final String[] DL_DECISIONS = { "dlWrongGroup", "dlPresetup", "dlNotPreAccept", "dlNoDeferral", "dlNotKeyTxn", "dlPositionMismatch", "dlNoCfk",
                                           "dlNotContended", "dlNoPenalty", "dlAlreadyDue", "dlAssigned" };
    static final String[] DL_CFK = { "dlCfkUnsafe", "dlCfkUnloaded", "dlCfkShrunk", "dlCfkRecent", "dlCfkQueued", "dlCfkQuiet" };
    static final String[] DL_FATE = { "dlDeferred", "dlExpiredBeforeDefer", "dlReleasedByHlc", "dlReleasedByReceipt" };
    static final String[] DL_TIMES = { "dlPlanned", "dlDeferredTime" };
    static final String[] ST_TASKS = { "runs", "batched", "handoffs" };
    static final String[] ST_CONT = { "continued", "contNoneWaiting", "contLockBusy", "contLimit", "contEmpty" };
    static final String[] ST_BATCH = { "batches", "batched", "inlined", "parked", "batchHeadIneligible", "batchNoCandidates", "batchNoFollowers",
                                       "batchRejectCost", "batchRejectNotFollower", "batchAfterRunLockBusy" };

    // per source (executor or store): the first and last values of each counter in the window
    static final class Span
    {
        final Map<String, long[]> v = new LinkedHashMap<>(); // name -> {first, last}
        int events;
        void add(String name, long value)
        {
            long[] x = v.get(name);
            if (x == null) v.put(name, new long[]{ value, value });
            else x[1] = value;
        }
        /** a counter that is reported only once non-zero: if it first appears after our first event, it started at zero */
        void addSparse(String name, long value)
        {
            long[] x = v.get(name);
            if (x == null) v.put(name, new long[]{ events <= 1 ? value : 0, value });
            else x[1] = value;
        }
        long delta(String name)
        {
            long[] x = v.get(name);
            return x == null ? 0 : Math.max(0, x[1] - x[0]);
        }
    }

    public static void main(String[] a) throws Exception
    {
        LocalTime from = null, to = null;
        boolean perStore = false;
        List<String> files = new ArrayList<>();
        for (int i = 0 ; i < a.length ; ++i)
        {
            switch (a[i])
            {
                case "--from": from = LocalTime.parse(a[++i]); break;
                case "--to": to = LocalTime.parse(a[++i]); break;
                case "--stores": perStore = true; break;
                default: files.add(a[i]);
            }
        }
        if (files.isEmpty())
        {
            System.err.println("usage: Features [--from HH:MM:SS[.sss]] [--to ...] [--stores] <jfr>...");
            System.exit(1);
        }

        for (String file : files)
        {
            TreeMap<Integer, Span> executors = new TreeMap<>();
            TreeMap<Integer, Span> stores = new TreeMap<>();
            TreeMap<Integer, Span> locks = new TreeMap<>();
            TreeMap<Integer, Integer> storeExecutor = new TreeMap<>();
            boolean hasDl = false, hasSt = false;
            long penalty = -1, maxDeferredNow = 0;
            Instant first = null, last = null;
            try (RecordingFile rf = new RecordingFile(Path.of(file)))
            {
                while (rf.hasMoreEvents())
                {
                    RecordedEvent e = rf.readEvent();
                    String name = e.getEventType().getName();
                    boolean isExecutor = name.equals("cassandra.accord.Executor");
                    if (!isExecutor && !name.equals("cassandra.accord.Store"))
                        continue;
                    LocalTime t = LocalTime.ofInstant(e.getStartTime(), ZoneOffset.UTC);
                    if ((from != null && t.isBefore(from)) || (to != null && t.isAfter(to)))
                        continue;
                    Instant at = e.getStartTime();
                    if (first == null || at.isBefore(first)) first = at;
                    if (last == null || at.isAfter(last)) last = at;
                    if (isExecutor && e.hasField("lockHeld") && e.getLong("lockAcquisitions") >= 0)
                    {
                        Span l = locks.computeIfAbsent(e.getInt("executorId"), k -> new Span());
                        l.add("lockAcquisitions", e.getLong("lockAcquisitions"));
                        l.add("lockHeld", e.getLong("lockHeld"));
                        if (e.getLong("lockHeldCpu") >= 0) l.add("lockHeldCpu", e.getLong("lockHeldCpu"));
                    }
                    if (isExecutor)
                    {
                        if (!e.hasField("dlAssigned")) continue;
                        hasDl = true;
                        Span s = executors.computeIfAbsent(e.getInt("executorId"), k -> new Span());
                        for (String f : DL_DECISIONS) s.add(f, e.hasField(f) ? e.getLong(f) : 0);
                        for (String f : DL_CFK) s.add(f, e.getLong(f));
                        for (String f : DL_FATE) s.add(f, e.getLong(f));
                        s.add("dlPlanned", e.getDuration("dlPlanned").toNanos() / 1000);
                        s.add("dlDeferredTime", e.getDuration("dlDeferredTime").toNanos());
                        long p = e.getDuration("deadlinePenalty").toNanos() / 1000;
                        penalty = p;
                        maxDeferredNow = Math.max(maxDeferredNow, e.getInt("deadlineDeferredNow"));
                    }
                    else
                    {
                        if (!e.hasField("handoffs") || e.getInt("commandStoreId") < 0) continue;
                        hasSt = true;
                        Span s = stores.computeIfAbsent(e.getInt("commandStoreId"), k -> new Span());
                        storeExecutor.put(e.getInt("commandStoreId"), e.getInt("executorId"));
                        ++s.events;
                        // the counters vary between builds (continuation was removed, batching's reasons were
                        // reworked), so take whichever of them this recording has, in the event's field order
                        for (jdk.jfr.ValueDescriptor d : e.getFields())
                        {
                            String f = d.getName();
                            if (d.getTypeName().equals("java.lang.String") && f.startsWith("batch"))
                            {
                                // a breakdown: space separated NAME=count pairs, non-zero counts only
                                String text = e.getString(f);
                                if (text == null || text.isEmpty()) continue;
                                for (String pair : text.split(" "))
                                {
                                    int eq = pair.lastIndexOf('=');
                                    if (eq > 0) s.addSparse(f + ":" + pair.substring(0, eq), Long.parseLong(pair.substring(eq + 1)));
                                }
                                continue;
                            }
                            if (!d.getTypeName().equals("long") && !d.getTypeName().equals("int")) continue;
                            if (f.equals("runs") || f.equals("handoffs") || f.equals("batches") || f.equals("batched") || f.equals("inlined")
                                || f.equals("parked") || f.equals("continued") || f.startsWith("cont") || f.startsWith("batch") || f.startsWith("single"))
                                s.add(f, e.getLong(f));
                        }
                    }
                }
            }

            double secs = first == null ? 0 : Math.max(1e-3, Duration.between(first, last).toMillis() / 1000.0);
            System.out.printf("%s  %s-%s (%.1fs)%n", Path.of(file).getFileName(), hms(first), hms(last), secs);
            if (!hasDl) System.out.println("  deadlines: n/a (no dl* fields on cassandra.accord.Executor)");
            else
            {
                Span sum = sum(executors.values());
                long decisions = 0;
                for (String f : DL_DECISIONS) decisions += sum.delta(f);
                System.out.printf("  deadlines: %d PreAccepts over keys (%.0f/s); penalty %s; deferred at once <= %d%n",
                                  decisions, decisions / secs, penalty < 0 ? "n/a" : String.format("%.1fms", penalty / 1000.0), maxDeferredNow);
                System.out.println("    decisions " + shares(sum, DL_DECISIONS, decisions));
                long cfks = 0;
                for (String f : DL_CFK) cfks += sum.delta(f);
                System.out.println("    cfk       " + shares(sum, DL_CFK, cfks));
                long assigned = sum.delta("dlAssigned"), released = sum.delta("dlReleasedByHlc") + sum.delta("dlReleasedByReceipt");
                System.out.println("    fate      " + shares(sum, DL_FATE, assigned).replace("%)", "% of assigned)")
                                   + String.format("  planned wait %s, deferred for %s (mean)",
                                                   assigned == 0 ? "-" : String.format("%.1fms", sum.delta("dlPlanned") / 1000.0 / assigned),
                                                   released == 0 ? "-" : String.format("%.1fms", sum.delta("dlDeferredTime") / 1e6 / released)));
            }

            if (!hasSt) System.out.println("  stores: n/a (no handoffs field on cassandra.accord.Store)");
            else
            {
                printStore("  stores (sum)", sum(stores.values()), secs);
                printBatchDetail(sum(stores.values()), secs);
                printLocks(locks, stores, storeExecutor, secs);
                if (perStore)
                    for (Map.Entry<Integer, Span> s : stores.entrySet())
                        printStore(String.format("    s%-3d", s.getKey()), s.getValue(), secs);
            }
        }
    }

    static void printStore(String label, Span s, double secs)
    {
        long runs = s.delta("runs"), batched = s.delta("batched"), handoffs = s.delta("handoffs");
        long tasks = runs + batched;
        System.out.printf("%s: %d tasks (%.0f/s) = %d runs + %d batched; %d hand-offs (%.0f/s), %.2f tasks per hand-off%n",
                          label, tasks, tasks / secs, runs, batched, handoffs, handoffs / secs, handoffs == 0 ? 0 : tasks / (double) handoffs);
        String pad = label.startsWith("  stores") ? "    " : "        ";
        List<String> cont = new ArrayList<>(), batch = new ArrayList<>();
        for (String f : s.v.keySet())
        {
            // the detail (breakdowns, sources, candidates, timings) is printed by printBatchDetail
            if (f.contains(":") || f.endsWith("Time") || f.endsWith("Candidates") || f.startsWith("batchFrom") || f.equals("batchFormCalls") || f.startsWith("single"))
                continue;
            if (f.equals("continued") || f.startsWith("cont")) cont.add(f);
            else if (f.equals("batches") || f.equals("batched") || f.equals("inlined") || f.equals("parked") || f.startsWith("batch")) batch.add(f);
        }
        if (!cont.isEmpty())
            System.out.println(pad + "continue  " + counts(s, cont.toArray(new String[0]), tasks) + "  (shares of tasks)");
        System.out.println(pad + "batch     " + counts(s, batch.toArray(new String[0]), tasks) + "  (shares of tasks)");
    }

    // the detail of batching from the breakdowns (absent from older builds)
    static void printBatchDetail(Span s, double secs)
    {
        if (s.delta("batchFormCalls") == 0 && !hasPrefix(s, "batchSizes:"))
            return;
        long batches = 0, members = 0;
        TreeMap<String, Long> sizes = breakdown(s, "batchSizes:");
        for (Map.Entry<String, Long> e : sizes.entrySet())
        {
            batches += e.getValue();
            String k = e.getKey();
            // a bucket's midpoint, for a mean
            double mid = k.endsWith("+") ? Double.parseDouble(k.substring(0, k.length() - 1))
                                         : k.contains("-") ? (Double.parseDouble(k.substring(0, k.indexOf('-'))) + Double.parseDouble(k.substring(k.indexOf('-') + 1))) / 2
                                                           : Double.parseDouble(k);
            members += Math.round(mid * e.getValue());
        }
        System.out.println("    batch sizes (tasks run incl. head)  " + pairs(sizes, batches, SIZE_ORDER) + String.format("  ~%.1f mean", batches == 0 ? 0 : members / (double) batches));
        long formed = sum(breakdown(s, "batchFormedLimit:"));
        System.out.println("    formation stopped by                " + pairs(breakdown(s, "batchFormedLimit:"), formed, null) + "  (first candidate refused; NONE: took all found)");
        long fromLanes = s.delta("batchFromLanes"), fromRunnable = s.delta("batchFromRunnable"), calls = s.delta("batchFormCalls");
        System.out.printf("    followers taken                     lanes=%d runnable=%d; candidates per attempt lanes=%.1f runnable=%.1f (%d attempts, %.0f/s)%n",
                          fromLanes, fromRunnable, calls == 0 ? 0 : s.delta("batchLaneCandidates") / (double) calls,
                          calls == 0 ? 0 : s.delta("batchRunnableCandidates") / (double) calls, calls, calls / secs);
        long reverted = sum(breakdown(s, "batchRevertedBy:"));
        if (reverted > 0)
            System.out.println("    reverted followers, by stop         " + pairs(breakdown(s, "batchRevertedBy:"), reverted, null));
        long parked = sum(breakdown(s, "batchParkedBy:"));
        if (parked > 0)
            System.out.println("    parked operations, by kind          " + pairs(breakdown(s, "batchParkedBy:"), parked, null));
        printReasons("    no follower could follow            ", breakdown(s, "batchNoFollowerReasons:"));
        printReasons("    head could not lead                 ", breakdown(s, "batchHeadReasons:"));
        printReasons("    deferred: consequence not inlinable ", breakdown(s, "batchDeferredReasons:"));
        printReasons("    after run: consequence not inlinable", breakdown(s, "batchAfterRunNotInlinableReasons:"));
        TreeMap<String, Long> kinds = breakdown(s, "batchNotInlinableKinds:");
        if (!kinds.isEmpty())
        {
            List<Map.Entry<String, Long>> top = new ArrayList<>(kinds.entrySet());
            top.sort((x, y) -> Long.compare(y.getValue(), x.getValue()));
            long total = sum(kinds);
            StringBuilder b = new StringBuilder();
            for (int i = 0 ; i < Math.min(8, top.size()) ; ++i)
                b.append(String.format("%s=%d(%.0f%%) ", top.get(i).getKey(), top.get(i).getValue(), 100.0 * top.get(i).getValue() / total));
            System.out.println("    not inlinable consequences (top)    " + b.toString().trim());
        }
        long formNanos = s.delta("batchFormTime"), completeNanos = s.delta("batchCompleteTime"), parkedNanos = s.delta("batchParkedRunTime");
        long singles = s.delta("singleCompletes"), singleNanos = s.delta("singleCompleteTime");
        long tasks = s.delta("runs") + s.delta("batched");
        System.out.printf("    lock time (ms/s)                    forming=%.1f (%.1fus/attempt) completing batches=%.1f (%.1fus/batch, %.1fus/task; parked ops %.1f) completing others=%.1f (%.1fus/task)%n",
                          formNanos / 1e6 / secs, calls == 0 ? 0 : formNanos / 1e3 / calls,
                          completeNanos / 1e6 / secs, batches == 0 ? 0 : completeNanos / 1e3 / batches,
                          members == 0 ? 0 : completeNanos / 1e3 / members, parkedNanos / 1e6 / secs,
                          singleNanos / 1e6 / secs, singles == 0 ? 0 : singleNanos / 1e3 / singles);
    }

    // the executor lock: wall and CPU time held per second, and per acquisition and per task of its stores
    static void printLocks(TreeMap<Integer, Span> locks, TreeMap<Integer, Span> stores, TreeMap<Integer, Integer> storeExecutor, double secs)
    {
        if (locks.isEmpty())
        {
            System.out.println("  lock: n/a (no lockHeld on cassandra.accord.Executor, or accord.debug.execution off)");
            return;
        }
        for (Map.Entry<Integer, Span> e : locks.entrySet())
        {
            Span l = e.getValue();
            long tasks = 0, handoffs = 0;
            for (Map.Entry<Integer, Span> st : stores.entrySet())
            {
                if (!e.getKey().equals(storeExecutor.get(st.getKey()))) continue;
                tasks += st.getValue().delta("runs") + st.getValue().delta("batched");
                handoffs += st.getValue().delta("handoffs");
            }
            long acq = l.delta("lockAcquisitions"), held = l.delta("lockHeld"), cpu = l.delta("lockHeldCpu");
            System.out.printf("  lock ex%d: held %.0fms/s wall, %s cpu; %.0f acquisitions/s (%.1fus wall each); %.1fus wall and %s cpu per task; %.2f acquisitions per task, %.2f per hand-off%n",
                              e.getKey(), held / 1e6 / secs, l.v.containsKey("lockHeldCpu") ? String.format("%.0fms/s", cpu / 1e6 / secs) : "n/a",
                              acq / secs, acq == 0 ? 0 : held / 1e3 / acq,
                              tasks == 0 ? 0 : held / 1e3 / tasks, l.v.containsKey("lockHeldCpu") && tasks > 0 ? String.format("%.1fus", cpu / 1e3 / tasks) : "n/a",
                              tasks == 0 ? 0 : acq / (double) tasks, handoffs == 0 ? 0 : acq / (double) handoffs);
        }
    }

    static final List<String> SIZE_ORDER = List.of("1", "2", "3", "4", "5-6", "7-8", "9-16", "17+");

    static boolean hasPrefix(Span s, String prefix)
    {
        for (String k : s.v.keySet()) if (k.startsWith(prefix)) return true;
        return false;
    }

    static TreeMap<String, Long> breakdown(Span s, String prefix)
    {
        TreeMap<String, Long> r = new TreeMap<>();
        for (String k : s.v.keySet())
        {
            if (!k.startsWith(prefix)) continue;
            long d = s.delta(k);
            if (d > 0) r.put(k.substring(prefix.length()), d);
        }
        return r;
    }

    static long sum(Map<String, Long> m)
    {
        long t = 0;
        for (long v : m.values()) t += v;
        return t;
    }

    static String pairs(Map<String, Long> m, long total, List<String> order)
    {
        StringBuilder b = new StringBuilder();
        List<String> keys = new ArrayList<>(m.keySet());
        if (order != null) keys.sort(Comparator.comparingInt(k -> order.indexOf(k) < 0 ? Integer.MAX_VALUE : order.indexOf(k)));
        else keys.sort((x, y) -> Long.compare(m.get(y), m.get(x)));
        for (String k : keys)
            b.append(String.format("%s=%d(%.0f%%) ", k, m.get(k), total == 0 ? 0 : 100.0 * m.get(k) / total));
        return b.toString().trim();
    }

    static void printReasons(String label, Map<String, Long> m)
    {
        if (!m.isEmpty())
            System.out.println(label + "  " + pairs(m, sum(m), null));
    }

    static String hms(Instant at)
    {
        return at == null ? "-" : LocalTime.ofInstant(at, ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MILLIS).toString();
    }

    static Span sum(Collection<Span> spans)
    {
        Span r = new Span();
        for (Span s : spans)
            for (String f : s.v.keySet())
            {
                long[] x = r.v.computeIfAbsent(f, k -> new long[2]);
                x[1] += s.delta(f);
            }
        return r;
    }

    static String shares(Span s, String[] fields, long total)
    {
        StringBuilder b = new StringBuilder();
        for (String f : fields)
        {
            long d = s.delta(f);
            b.append(String.format("%s=%d(%.0f%%) ", strip(f), d, total == 0 ? 0 : 100.0 * d / total));
        }
        return b.toString().trim();
    }

    static String counts(Span s, String[] fields, long total)
    {
        StringBuilder b = new StringBuilder();
        for (String f : fields)
        {
            long d = s.delta(f);
            b.append(String.format("%s=%d(%.0f%%) ", strip(f), d, total == 0 ? 0 : 100.0 * d / total));
        }
        return b.toString().trim();
    }

    static String strip(String f)
    {
        String r = f.startsWith("dl") ? f.substring(2) : f;
        return Character.toLowerCase(r.charAt(0)) + r.substring(1);
    }
}
