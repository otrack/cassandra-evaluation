import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// ExecutorWaits [--from HH:MM:SS[.sss]] [--to ...] [--bucket ms] [--min-ms M] <jfr> : what the Accord executor threads
// (AccordExecutorSignalLoop) are doing when work queues up, to tell a busy dispatcher from a wakeup problem:
//   per bucket (default 1000ms), executor wall samples (weighted by batch size) by state:
//     inLock      in the dispatch critical section (below LoopTask.*ExclusiveAndUnlock)
//     task        running a task
//     await-park  blocked in SignalLock.awaitAsyncOrLock, waiting for an async-work signal or the lock
//     await-spin  spinning there (queue_spin_interval set)
//     lock()      waiting unconditionally in SignalLock.lock / tryLock
//   non-executor threads waiting in SignalLock.lock / tryLock (admin paths: taskSnapshot, waitForQuiescence, ...)
//   the longest AccordExecutorCriticalSection spans (>= --min-ms, default 20; sampled spans, so long-biased), with how
//   much of each the holder spent on CPU (a low share = descheduled, or stalled e.g. on allocation)
//   and, over the critical sections of >= 50ms, what the holder's CPU went to: completing finished tasks or submitting
//   new ones (draining the pending list), preparing ready tasks, polling, or unlocking and waking threads
// Many threads in await-park while the holder only drains pending work (and prepares nothing) means the readyToRun
// queue is empty because the holder is not refilling it, rather than waiters missing signals.
public class ExecutorWaits
{
    public static void main(String[] args) throws Exception
    {
        Dx.Window window = new Dx.Window();
        String file = null;
        long bucket = 1000, minMs = 20;
        for (int i = 0; i < args.length; )
        {
            int n = window.accept(args, i);
            if (n > 0) { i += n; continue; }
            switch (args[i])
            {
                case "--bucket": bucket = Long.parseLong(args[i + 1]); i += 2; break;
                case "--min-ms": minMs = Long.parseLong(args[i + 1]); i += 2; break;
                default: file = args[i++];
            }
        }
        if (file == null)
        {
            System.err.println("usage: ExecutorWaits " + Dx.Window.usage() + " [--bucket ms] [--min-ms M] <jfr>");
            System.exit(2);
        }

        TreeMap<Long, Map<String, Long>> states = new TreeMap<>();
        Map<String, Long> otherLockWaiters = new TreeMap<>();
        record Section(long startMicros, long durMicros, String thread) {}
        List<Section> sections = new ArrayList<>();
        Map<String, List<long[]>> cpu = new HashMap<>(); // thread -> [micros, phase index]
        List<String> phases = new ArrayList<>();
        try (RecordingFile rf = new RecordingFile(Path.of(file)))
        {
            while (rf.hasMoreEvents())
            {
                RecordedEvent e = rf.readEvent();
                String ev = e.getEventType().getName();
                if (!window.contains(e.getStartTime())) continue;
                switch (ev)
                {
                    case "profiler.Span":
                        if (Tags.isCriticalSection(e.getString("tag")) && e.getDuration().toMillis() >= minMs)
                            sections.add(new Section(micros(e.getStartTime()), e.getDuration().toNanos() / 1000, Threads.name(e.getThread())));
                        break;
                    case "jdk.ExecutionSample":
                    {
                        String name = Threads.name(Threads.of(e));
                        if (!name.startsWith("AccordExecutor")) break;
                        String p = phase(e.getStackTrace());
                        int idx = phases.indexOf(p);
                        if (idx < 0) { phases.add(p); idx = phases.size() - 1; }
                        cpu.computeIfAbsent(name, k -> new ArrayList<>()).add(new long[]{ micros(e.getStartTime()), idx });
                        break;
                    }
                    case "profiler.WallClockSample":
                    {
                        String name = Threads.name(Threads.of(e));
                        long w = e.getLong("samples");
                        String s = state(e.getStackTrace());
                        if (name.startsWith("AccordExecutor"))
                        {
                            final long b = bucket;
                            states.computeIfAbsent(e.getStartTime().toEpochMilli() / b, k -> new TreeMap<>()).merge(s, w, Long::sum);
                        }
                        else if (s.equals("lock()"))
                        {
                            otherLockWaiters.merge(Threads.fold(name), w, Long::sum);
                        }
                        break;
                    }
                }
            }
        }

        System.out.println(Path.of(file).getFileName() + "  (" + window.describe() + ")");
        System.out.println("executor wall samples by state, per " + bucket + "ms");
        for (var en : states.entrySet())
            System.out.println("  " + LocalTime.ofInstant(Instant.ofEpochMilli(en.getKey() * bucket), ZoneOffset.UTC) + " " + en.getValue());
        System.out.println("non-executor threads waiting in SignalLock.lock/tryLock (wall samples): " + otherLockWaiters);

        sections.sort((x, y) -> Long.compare(y.durMicros, x.durMicros));
        System.out.println("critical sections >= " + minMs + "ms (sampled): " + sections.size());
        for (int i = 0; i < Math.min(15, sections.size()); i++)
        {
            Section s = sections.get(i);
            long on = cpu.getOrDefault(s.thread, List.of()).stream().filter(x -> x[0] >= s.startMicros && x[0] < s.startMicros + s.durMicros).count();
            System.out.printf("  %s %-22s %7.1fms  holder on-CPU %4d ms (%.0f%%)%n", LocalTime.ofInstant(Instant.ofEpochMilli(s.startMicros / 1000), ZoneOffset.UTC),
                              s.thread, s.durMicros / 1000.0, on, 100.0 * on * 1000 / s.durMicros);
        }
        Map<String, Long> byPhase = new TreeMap<>();
        long total = 0;
        for (Section s : sections)
        {
            if (s.durMicros < 50_000) continue;
            for (long[] x : cpu.getOrDefault(s.thread, List.of()))
                if (x[0] >= s.startMicros && x[0] < s.startMicros + s.durMicros) { byPhase.merge(phases.get((int) x[1]), 1L, Long::sum); total++; }
        }
        System.out.println("holder CPU inside critical sections >= 50ms, by phase (" + total + " ms):");
        for (var en : byPhase.entrySet())
            System.out.printf("  %5.1f%%  %s%n", 100.0 * en.getValue() / Math.max(1, total), en.getKey());
    }

    static long micros(Instant t)
    {
        return t.getEpochSecond() * 1_000_000L + t.getNano() / 1000;
    }

    static String simple(RecordedFrame f)
    {
        return f.getMethod().getType().getName().replaceAll("^.*\\.", "") + "." + f.getMethod().getName();
    }

    /** what an executor thread is doing, from its innermost relevant frame */
    static String state(RecordedStackTrace st)
    {
        if (st == null) return "?";
        List<RecordedFrame> fr = st.getFrames();
        for (int i = 0; i < fr.size(); i++)
        {
            String m = simple(fr.get(i));
            if (m.endsWith("ExclusiveAndUnlock")) return "inLock";
            if (m.equals("SignalLock.lock") || m.equals("SignalLock.tryLock")) return "lock()";
            if (m.equals("SignalLock.awaitAsyncOrLock"))
            {
                for (int j = 0; j < i; j++)
                {
                    String n = fr.get(j).getMethod().getName();
                    if (n.equals("parkNanos")) return "await-spin";
                    if (n.equals("park")) return "await-park";
                }
                return "await-run";
            }
            if (m.equals("ExclusiveExecutor.runTask") || m.equals("Task.runNoExcept")) return "task";
        }
        return "other";
    }

    /** for a CPU sample: which part of the critical section it is in, from the outermost relevant frame */
    static String phase(RecordedStackTrace st)
    {
        if (st == null) return "?";
        List<RecordedFrame> fr = st.getFrames();
        String p = null;
        for (int i = fr.size() - 1; i >= 0; i--)
        {
            String m = simple(fr.get(i));
            if (m.endsWith("ExclusiveAndUnlock")) p = "lock: other";
            if (p == null) continue;
            if (m.equals("Task.submitExclusiveNoExcept")) return "lock: submit (new task)";
            if (m.equals("Task.completeExclusiveNoExcept")) return "lock: complete (finished task)";
            if (m.equals("Task.prepareExclusiveNoExcept")) return "lock: prepare (ready task)";
            if (m.equals("AccordExecutor.pollAlreadyWaitingToRunExclusive")) return "lock: poll";
            if (m.equals("SignalLock.unlock") || m.equals("SignalLock.unlockAndAcquireAsyncWork")) return "lock: unlock/wake";
        }
        return p == null ? "not in lock" : p;
    }
}
