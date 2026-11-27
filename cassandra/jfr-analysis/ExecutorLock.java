import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// ExecutorLock [--from HH:MM:SS[.sss]] [--to ...] <jfr> : how busy each Accord executor shard's lock is, per second.
// AccordExecutorSignalLoop dispatches under one lock per executor shard (queue_shard_count), so its critical section
// is serial work for every task of the shard:
//   heldCPU   executor CPU samples (1ms each) inside the critical section: below LoopTask.*ExclusiveAndUnlock
//             (completing a task and fetching the next ones), i.e. CPU time with the lock held, including its release
//             (exitLockAndAcquireAsyncWork, which may wake other threads)
//   waitCPU   samples spinning in SignalLock.lock/tryLock (parking while waiting is off-CPU, so not seen here)
//   execCPU   all executor CPU of the shard
//   CSspans   summed AccordExecutorCriticalSection spans: wall time, but only of the sampled spans (a lower bound)
// heldCPU approaching 1000ms/s means the shard's dispatch is saturated; wall time held can exceed it when the holder
// is descheduled (see HostPressure), which -Daccord.debug_execution_report=true logs as "Held lock ... with cpu time
// only ...". Then, what the critical section was doing (the frame below its entry point).
public class ExecutorLock
{
    public static void main(String[] args) throws Exception
    {
        Dx.Window window = new Dx.Window();
        String file = null;
        for (int i = 0; i < args.length; )
        {
            int n = window.accept(args, i);
            if (n > 0) i += n;
            else file = args[i++];
        }
        if (file == null)
        {
            System.err.println("usage: ExecutorLock " + Dx.Window.usage() + " <jfr>");
            System.exit(2);
        }

        TreeMap<String, long[]> m = new TreeMap<>(); // "HH:MM:SS sN" -> held, wait, exec, csNanos
        Map<String, Long> heldBy = new HashMap<>();
        try (RecordingFile rf = new RecordingFile(Path.of(file)))
        {
            while (rf.hasMoreEvents())
            {
                RecordedEvent e = rf.readEvent();
                String ev = e.getEventType().getName();
                boolean span = ev.equals("profiler.Span");
                if (!span && !ev.equals("jdk.ExecutionSample")) continue;
                if (!window.contains(e.getEndTime())) continue;
                String name = Threads.name(Threads.of(e));
                if (!name.startsWith("AccordExecutor[")) continue;
                String shard = name.substring("AccordExecutor[".length(), name.indexOf(','));
                String key = LocalTime.ofSecondOfDay(e.getEndTime().getEpochSecond() % 86400) + " s" + shard;
                long[] r = m.computeIfAbsent(key, k -> new long[4]);
                if (span)
                {
                    if (Tags.isCriticalSection(e.getString("tag"))) r[3] += e.getDuration().toNanos();
                    continue;
                }
                r[2]++;
                RecordedStackTrace st = e.getStackTrace();
                if (st == null) continue;
                List<RecordedFrame> fr = st.getFrames();
                boolean waiting = false;
                for (int i = 0; i < fr.size(); i++)
                {
                    String mn = fr.get(i).getMethod().getType().getName() + "." + fr.get(i).getMethod().getName();
                    if (mn.endsWith("ExclusiveAndUnlock"))
                    {
                        r[0]++;
                        String below = i == 0 ? "?" : fr.get(i - 1).getMethod().getType().getName().replaceAll("^.*\\.", "") + "." + fr.get(i - 1).getMethod().getName();
                        heldBy.merge(below, 1L, Long::sum);
                        waiting = false;
                        break;
                    }
                    if (mn.endsWith("SignalLock.lock") || mn.endsWith("SignalLock.tryLock")) waiting = true;
                }
                if (waiting) r[1]++;
            }
        }

        System.out.println(Path.of(file).getFileName() + "  (" + window.describe() + ")");
        System.out.println("second   shard  heldCPU(ms) waitCPU(ms) execCPU(ms) CSspans(ms)");
        long[] tot = new long[4];
        for (var en : m.entrySet())
        {
            long[] r = en.getValue();
            for (int i = 0; i < 4; i++) tot[i] += r[i];
            System.out.printf("%-14s %10d %11d %11d %11d%n", en.getKey(), r[0], r[1], r[2], r[3] / 1_000_000);
        }
        int n = Math.max(1, m.size());
        System.out.printf("per shard-second %4d %11d %11d %11d%n", tot[0] / n, tot[1] / n, tot[2] / n, tot[3] / 1_000_000 / n);
        System.out.println("-- in the critical section: the frame below its entry point (CPU ms)");
        heldBy.entrySet().stream().sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(10)
              .forEach(x -> System.out.println("  " + x.getValue() + "  " + x.getKey()));
    }
}
