import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// QueueTimeline [--bucket ms] [--from HH:MM:SS[.sss]] [--to ...] <jfr> : per bucket (default 1000ms), the queueing of
// Accord's store tasks next to what could explain it, to tell executor dispatch problems from CPU starvation:
//   qEnd/q>100/qMax   "Queued <task> s<store>" spans ending in the bucket (creation to running: pending, loading,
//                     waiting on keys/txns, waiting behind the store, at the head), how many took >= 100ms, the longest
//   q>100@start       of those >= 100ms, how many started in the bucket (a burst of starts = one stall)
//   headMax           longest "Head" span (at the head of its store until running: handing it to a thread)
//   critMax           longest AccordExecutorCriticalSection (executor lock hold)
//   runs              store task run spans
//   CPU (cores)       executor / messaging / JIT (C1+C2) / GC / other threads, from jdk.ExecutionSample (1 sample = 1ms)
//   exWall run/all    executor wall samples RUNNABLE / all (weighted by batch size)
//   wait/prep/run     cassandra.accord.Executor events (summed over executors, and over the samples in the bucket)
//   node%             jdk.CPULoad machineTotal (the whole host, including any co-located client)
//   ldPause           cassandra.accord.Executor samples (all executors) with loading paused because the cache is over
//                     capacity plus working set / all samples (needs a build whose event has loadingPaused)
//   pendMax/defMax    largest pending list (tasks awaiting submission or completion under the lock) and deferred
//                     completions (queue_completion_budget exceeded) in any executor sample (builds with those fields)
//   wkMax             largest count of tasks waiting on another task's transaction or key (WAITING_ON_TXN/KEY) in any
//                     executor sample (builds with waitingOnTxnOrKey)
// Spans are emitted only if the profiler sampled them (long-biased): use the maxima, not the counts, unless the
// capture had -Daccord.debug.execution.spans=all.
public class QueueTimeline
{
    public static void main(String[] args) throws Exception
    {
        long bucketMs = 1000;
        Dx.Window window = new Dx.Window();
        String file = null;
        for (int i = 0; i < args.length; )
        {
            int n = window.accept(args, i);
            if (n > 0) { i += n; continue; }
            if (args[i].equals("--bucket")) { bucketMs = Long.parseLong(args[i + 1]); i += 2; continue; }
            file = args[i++];
        }
        if (file == null)
        {
            System.err.println("usage: QueueTimeline [--bucket ms] " + Dx.Window.usage() + " <jfr>");
            System.exit(2);
        }

        final int Q_END = 0, Q_SLOW = 1, Q_MAX = 2, Q_SLOW_START = 3, HEAD_MAX = 4, CRIT_MAX = 5, RUNS = 6,
                  CPU_EX = 7, CPU_MSG = 8, CPU_JIT = 9, CPU_OTHER = 10, WALL_RUN = 11, WALL_ALL = 12,
                  WAIT = 13, PREP = 14, RUN = 15, NODE = 16, NODE_N = 17, CPU_GC = 18, EX_SAMPLES = 19, LOAD_PAUSED = 20, PEND_MAX = 21, DEFER_MAX = 22, WAITING_MAX = 23, N = 24;
        TreeMap<Long, long[]> m = new TreeMap<>();
        try (RecordingFile rf = new RecordingFile(Path.of(file)))
        {
            while (rf.hasMoreEvents())
            {
                RecordedEvent e = rf.readEvent();
                String ev = e.getEventType().getName();
                Instant end = e.getEndTime();
                if (!window.contains(end)) continue;
                final long bms = bucketMs;
                long[] r = m.computeIfAbsent(end.toEpochMilli() / bms, k -> new long[N]);
                switch (ev)
                {
                    case "profiler.Span":
                    {
                        String tag = e.getString("tag");
                        long d = e.getDuration().toMillis();
                        if (Tags.isDx(tag)) break;
                        if (Tags.isQueued(tag) && Tags.isStoreTask(tag))
                        {
                            r[Q_END]++;
                            r[Q_MAX] = Math.max(r[Q_MAX], d);
                            if (d >= 100)
                            {
                                r[Q_SLOW]++;
                                m.computeIfAbsent(e.getStartTime().toEpochMilli() / bms, k -> new long[N])[Q_SLOW_START]++;
                            }
                        }
                        else if (Tags.isHead(tag)) r[HEAD_MAX] = Math.max(r[HEAD_MAX], d);
                        else if (Tags.isCriticalSection(tag)) r[CRIT_MAX] = Math.max(r[CRIT_MAX], d);
                        else if (Tags.isRun(tag) && Tags.isStoreTask(tag)) r[RUNS]++;
                        break;
                    }
                    case "jdk.ExecutionSample":
                    {
                        String c = Threads.category(e);
                        if (c.equals("AccExec")) r[CPU_EX]++;
                        else if (c.equals("MsgLoop")) r[CPU_MSG]++;
                        else if (Threads.isJit(c)) r[CPU_JIT]++;
                        else if (c.equals("GC")) r[CPU_GC]++;
                        else r[CPU_OTHER]++;
                        break;
                    }
                    case "profiler.WallClockSample":
                    {
                        if (!Threads.name(Threads.of(e)).startsWith("AccordExecutor")) break;
                        long s = e.getLong("samples");
                        r[WALL_ALL] += s;
                        if (e.getString("state").contains("RUNNABLE")) r[WALL_RUN] += s;
                        break;
                    }
                    case "cassandra.accord.Executor":
                        r[WAIT] += e.getLong("waitingToRun");
                        r[PREP] += e.getLong("preparingToRun");
                        r[RUN] += e.getLong("running");
                        r[EX_SAMPLES]++;
                        if (e.hasField("loadingPaused") && e.getBoolean("loadingPaused")) r[LOAD_PAUSED]++;
                        if (e.hasField("waitingOnTxnOrKey")) r[WAITING_MAX] = Math.max(r[WAITING_MAX], e.getInt("waitingOnTxnOrKey"));
                        if (e.hasField("pending")) r[PEND_MAX] = Math.max(r[PEND_MAX], e.getInt("pending"));
                        if (e.hasField("deferredCompletions")) r[DEFER_MAX] = Math.max(r[DEFER_MAX], e.getInt("deferredCompletions"));
                        break;
                    case "jdk.CPULoad":
                        r[NODE] += Math.round(e.getDouble("machineTotal") * 1000);
                        r[NODE_N]++;
                        break;
                }
            }
        }

        double cores = bucketMs; // samples per core per bucket
        System.out.println(Path.of(file).getFileName() + "  (" + window.describe() + ", " + bucketMs + "ms buckets)");
        System.out.println("bucket          qEnd  q>100   qMax q>100@start headMax critMax   runs   exCPU msgCPU jitCPU  gcCPU othCPU  exWall run/all   wait  prep   run  node%  ldPause  pendMax defMax  wkMax");
        for (var en : m.entrySet())
        {
            long[] r = en.getValue();
            String ts = LocalTime.ofInstant(Instant.ofEpochMilli(en.getKey() * bucketMs), ZoneOffset.UTC).toString();
            System.out.printf("%-14s %6d %6d %6d %11d %7d %7d %6d  %6.2f %6.2f %6.2f %6.2f %6.2f  %7d/%-7d %5d %5d %5d %5s  %7s  %7d %6d %6d%n",
                              ts, r[Q_END], r[Q_SLOW], r[Q_MAX], r[Q_SLOW_START], r[HEAD_MAX], r[CRIT_MAX], r[RUNS],
                              r[CPU_EX] / cores, r[CPU_MSG] / cores, r[CPU_JIT] / cores, r[CPU_GC] / cores, r[CPU_OTHER] / cores,
                              r[WALL_RUN], r[WALL_ALL], r[WAIT], r[PREP], r[RUN],
                              r[NODE_N] == 0 ? "" : String.format("%.0f", r[NODE] / 10.0 / r[NODE_N]),
                              r[EX_SAMPLES] == 0 ? "" : r[LOAD_PAUSED] + "/" + r[EX_SAMPLES], r[PEND_MAX], r[DEFER_MAX], r[WAITING_MAX]);
        }
    }
}
