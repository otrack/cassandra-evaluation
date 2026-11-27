import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// WorkRate [--from HH:MM:SS[.sss]] [--to ...] <jfr>... : per second, a throughput proxy next to the CPU of each kind
// of thread, and the executor CPU per unit of work:
//   KApp     "DX K Applied" spans: keys are sampled by hash (-Daccord.debug.distributed.execution.keys=K), independently
//            of which transactions are traced, so this tracks the node's applied transactions (x K / keys per txn)
//   Client   "DX C Client" spans ending (transactions coordinated here, sampled 1 in N by TxnId)
//   CPU      cores (jdk.ExecutionSample, 1 sample = 1ms) of the executors, messaging event loops, C1 and C2 JIT
//            compiler threads, GC, and everything else (see Threads.category)
//   ex/KApp  executor CPU ms per K Applied: rises when the same work costs more (e.g. code not yet compiled by C2)
// then the totals over the window.
public class WorkRate
{
    static final String[] CATS = { "AccExec", "MsgLoop", "C1", "C2", "GC" };

    public static void main(String[] args) throws Exception
    {
        Dx.Window window = new Dx.Window();
        List<String> files = new ArrayList<>();
        for (int i = 0; i < args.length; )
        {
            int n = window.accept(args, i);
            if (n > 0) i += n;
            else files.add(args[i++]);
        }
        if (files.isEmpty())
        {
            System.err.println("usage: WorkRate " + Dx.Window.usage() + " <jfr>...");
            System.exit(2);
        }
        for (String file : files)
        {
            TreeMap<Long, long[]> m = new TreeMap<>(); // KApp, Client, cats..., other
            int n = CATS.length + 3;
            try (RecordingFile rf = new RecordingFile(Path.of(file)))
            {
                while (rf.hasMoreEvents())
                {
                    RecordedEvent e = rf.readEvent();
                    String ev = e.getEventType().getName();
                    boolean span = ev.equals("profiler.Span");
                    if (!span && !ev.equals("jdk.ExecutionSample")) continue;
                    Instant t = span ? e.getEndTime() : e.getStartTime();
                    if (!window.contains(t)) continue;
                    long[] r = m.computeIfAbsent(t.getEpochSecond(), k -> new long[n]);
                    if (span)
                    {
                        String tag = e.getString("tag");
                        if (tag.startsWith("DX K Applied")) r[0]++;
                        else if (tag.startsWith("DX C Client")) r[1]++;
                        continue;
                    }
                    String c = Threads.category(e);
                    int idx = Arrays.asList(CATS).indexOf(c);
                    if (c.equals("JIT")) idx = Arrays.asList(CATS).indexOf("C2"); // unattributed compiler work
                    r[idx < 0 ? n - 1 : 2 + idx]++;
                }
            }
            System.out.println(Path.of(file).getFileName() + "  (" + window.describe() + ")");
            System.out.println("second      KApp Client   exCPU msgCPU  c1CPU  c2CPU  gcCPU othCPU  ex/KApp(ms)");
            long[] tot = new long[n];
            for (var en : m.entrySet())
            {
                long[] r = en.getValue();
                for (int i = 0; i < n; i++) tot[i] += r[i];
                print(LocalTime.ofSecondOfDay(en.getKey() % 86400).toString(), r, 1);
            }
            print("TOTAL/s", tot, Math.max(1, m.size()));
        }
    }

    static void print(String label, long[] r, int secs)
    {
        System.out.printf("%-10s %5.0f %6.1f  %6.2f %6.2f %6.2f %6.2f %6.2f %6.2f  %8.1f%n", label,
                          (double) r[0] / secs, (double) r[1] / secs,
                          r[2] / 1000.0 / secs, r[3] / 1000.0 / secs, r[4] / 1000.0 / secs, r[5] / 1000.0 / secs, r[6] / 1000.0 / secs, r[7] / 1000.0 / secs,
                          r[0] == 0 ? Double.NaN : (double) r[2] / r[0]);
    }
}
