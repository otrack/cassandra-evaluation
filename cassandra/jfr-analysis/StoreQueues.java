import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// StoreQueues [--from HH:MM:SS[.sss]] [--to ...] [--bucket ms] [--stores LIST] [--waiting N] <jfr> :
// the per command store events of builds with cassandra.accord.Store / cassandra.accord.WaitingTask (JfrDiagnostics,
// taken under each executor's lock every 100 ms):
//   per bucket (default 1000ms) and store: the largest number of tasks waiting on another task's txn or key ("w"),
//   loading ("l") and queued at the store ("q"), the oldest waiting age (ms), and the store's busy share (its tasks'
//   run time / the bucket, from the cumulative counters), tasks run per second and their mean run time (us); a store is printed for a bucket only if something waited
//   >= 50ms or it was >= 50% busy, to keep the output to the stores that matter (--stores to pick some anyway)
//   then, with --waiting N (default 40), the N oldest waiting tasks reported (of --stores, if given), grouped by
//   (store, entry, holder, queue head): what each waits on, and whom
public class StoreQueues
{
    public static void main(String[] a) throws Exception
    {
        LocalTime from = null, to = null;
        long bucketMs = 1000;
        int waitingTop = 40;
        Set<Integer> pick = new HashSet<>();
        String file = null;
        for (int i = 0 ; i < a.length ; ++i)
        {
            switch (a[i])
            {
                case "--from": from = LocalTime.parse(a[++i]); break;
                case "--to": to = LocalTime.parse(a[++i]); break;
                case "--bucket": bucketMs = Long.parseLong(a[++i]); break;
                case "--waiting": waitingTop = Integer.parseInt(a[++i]); break;
                case "--stores": for (String s : a[++i].split(",")) pick.add(Integer.parseInt(s.trim())); break;
                default: file = a[i];
            }
        }
        if (file == null)
        {
            System.err.println("usage: StoreQueues [--from HH:MM:SS[.sss]] [--to ...] [--bucket ms] [--stores LIST] [--waiting N] <jfr>");
            System.exit(1);
        }

        // bucket -> store -> {maxWaiting, maxLoading, maxQueued, maxOldestWaitingNanos, firstRuns, firstRunNanos, lastRunNanos, samples}
        TreeMap<Long, TreeMap<Integer, long[]>> buckets = new TreeMap<>();
        Map<String, long[]> waits = new HashMap<>(); // group -> {count, maxAgeNanos}
        Map<String, String> example = new HashMap<>();
        long stores = 0, waitingEvents = 0;
        try (RecordingFile rf = new RecordingFile(Path.of(file)))
        {
            while (rf.hasMoreEvents())
            {
                RecordedEvent e = rf.readEvent();
                String name = e.getEventType().getName();
                boolean isStore = name.equals("cassandra.accord.Store");
                if (!isStore && !name.equals("cassandra.accord.WaitingTask"))
                    continue;
                LocalTime t = LocalTime.ofInstant(e.getStartTime(), ZoneOffset.UTC);
                if ((from != null && t.isBefore(from)) || (to != null && t.isAfter(to)))
                    continue;
                int store = e.getInt("commandStoreId");
                if (isStore)
                {
                    ++stores;
                    long b = e.getStartTime().toEpochMilli() / bucketMs;
                    long[] v = buckets.computeIfAbsent(b, k -> new TreeMap<>()).computeIfAbsent(store, k -> new long[]{ 0, 0, 0, 0, -1, -1, 0, 0, 0 });
                    v[0] = Math.max(v[0], e.getInt("waiting"));
                    v[1] = Math.max(v[1], e.getInt("loading"));
                    v[2] = Math.max(v[2], e.getInt("queued"));
                    v[3] = Math.max(v[3], e.getLong("oldestWaiting"));
                    if (v[4] < 0) { v[4] = e.getLong("runs"); v[5] = e.getLong("runTime"); }
                    v[6] = e.getLong("runTime");
                    v[8] = e.getLong("runs");
                    v[7]++;
                }
                else
                {
                    if (!pick.isEmpty() && !pick.contains(store))
                        continue;
                    ++waitingEvents;
                    String key = "s" + store + "  waits on " + e.getString("entry")
                                 + "  held by " + (e.getString("holder") == null ? "-" : e.getString("holder") + " [" + e.getString("holderState") + "]")
                                 + "  head " + e.getString("queueHead");
                    long[] v = waits.computeIfAbsent(key, k -> new long[2]);
                    v[0]++;
                    long age = e.getLong("age");
                    if (age > v[1])
                    {
                        v[1] = age;
                        example.put(key, String.format("%s %s %s fifo=%s refs=%d notHeld=%d%s queue=%d fifoSize=%d runnable=%d index=%d%s at %s",
                                                       e.getString("task"), e.getString("txnId"), e.getString("state"), e.getBoolean("fifo"),
                                                       e.getInt("refs"), e.getInt("notHeld"), e.hasField("blockedOn") ? " blockedOn=" + e.getInt("blockedOn") : "", e.getInt("queueSize"), e.getInt("fifoSize"),
                                                       e.getInt("runnablePrefix"), e.getInt("indexInQueue"),
                                                       e.hasField("headInBlocking") && e.getInt("headInBlocking") >= 0
                                                       ? String.format(" head[inBlocking=%d inNotBlocking=%d blocking=%d notBlocking=%d processed=%d/%d]",
                                                                       e.getInt("headInBlocking"), e.getInt("headInNotBlocking"), e.getInt("headBlocking"),
                                                                       e.getInt("headNotBlocking"), e.getInt("headProcessed"), e.getInt("headKeys"))
                                                       : "", t));
                    }
                }
            }
        }
        System.out.printf("%s: %d store samples, %d waiting-task samples%n", Path.of(file).getFileName(), stores, waitingEvents);
        if (stores == 0)
        {
            System.out.println("(no cassandra.accord.Store events: needs a build with them, and diagnostics.jfc enabling them)");
            return;
        }
        System.out.println("bucket       store  w(max) l(max) q(max) oldestWait(ms) busy%  runs/s  us/run");
        TreeMap<Integer, long[]> prev = null;
        for (var en : buckets.entrySet())
        {
            for (var se : en.getValue().entrySet())
            {
                long[] v = se.getValue();
                long base = v[5], baseRuns = v[4];
                if (prev != null && prev.containsKey(se.getKey())) { base = prev.get(se.getKey())[6]; baseRuns = prev.get(se.getKey())[8]; }
                double busy = 100.0 * (v[6] - base) / (bucketMs * 1e6);
                long runs = v[8] - baseRuns;
                boolean show = pick.contains(se.getKey()) || v[3] >= 50_000_000L || busy >= 50;
                if (!show) continue;
                System.out.printf("%s  s%-4d %6d %6d %6d %14d %5.0f %7.0f %7.0f%n", LocalTime.ofInstant(Instant.ofEpochMilli(en.getKey() * bucketMs), ZoneOffset.UTC),
                                  se.getKey(), v[0], v[1], v[2], v[3] / 1_000_000, busy, runs * 1000.0 / bucketMs, runs == 0 ? 0 : (v[6] - base) / 1000.0 / runs);
            }
            prev = en.getValue();
        }
        if (waitingTop > 0 && !waits.isEmpty())
        {
            System.out.println("-- waiting tasks reported (oldest per executor per sample), by what they wait on: count, max age (ms), oldest example");
            waits.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue()[1], x.getValue()[1])).limit(waitingTop)
                 .forEach(en -> System.out.printf("  %5d %6d  %s%n               %s%n", en.getValue()[0], en.getValue()[1] / 1_000_000, en.getKey(), example.get(en.getKey())));
        }
    }
}
