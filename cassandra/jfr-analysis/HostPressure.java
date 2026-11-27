import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

// HostPressure [--from HH:MM:SS[.sss]] [--to ...] <jfr>... : per second, whether the node's threads were waiting for
// a CPU, which CPU samples (jdk.ExecutionSample) cannot show:
//   cpuSome/cpuFull  cgroup PSI CPU stall time (ms per second: some / all runnable tasks waiting for a core), from
//                    the cumulative cassandra.os.Pressure counters (cassandra.jfr.diagnostic_events)
//   steal            host CPU steal (ms per second)
//   running          threads running or runnable (cassandra.os.Threads), mean/max over the samples in the second
//   node%/jvm%       jdk.CPULoad machineTotal and jvmUser+jvmSystem
//   throttled        jdk.ContainerCPUThrottling: throttled slices / time in the second (CPU quota exhausted)
public class HostPressure
{
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
            System.err.println("usage: HostPressure " + Dx.Window.usage() + " <jfr>...");
            System.exit(2);
        }
        for (String file : files)
        {
            // per second: [0..2] last cumulative cpuSome, cpuFull, steal (ms; NaN if none), [3..5] running sum/max/n,
            // [6..8] CPULoad machine/jvm/n, [9..10] last cumulative throttled slices/ms (NaN if none)
            TreeMap<Long, double[]> m = new TreeMap<>();
            try (RecordingFile rf = new RecordingFile(Path.of(file)))
            {
                while (rf.hasMoreEvents())
                {
                    RecordedEvent e = rf.readEvent();
                    String ev = e.getEventType().getName();
                    if (!window.contains(e.getStartTime())) continue;
                    double[] r = m.computeIfAbsent(e.getStartTime().getEpochSecond(), k -> {
                        double[] x = new double[11];
                        x[0] = x[1] = x[2] = x[9] = x[10] = Double.NaN;
                        return x;
                    });
                    switch (ev)
                    {
                        case "cassandra.os.Pressure":
                            r[0] = ms(e, "cpuSome"); r[1] = ms(e, "cpuFull"); r[2] = ms(e, "hostSteal");
                            break;
                        case "cassandra.os.Threads":
                            r[3] += e.getInt("running"); r[4] = Math.max(r[4], e.getInt("running")); r[5]++;
                            break;
                        case "jdk.CPULoad":
                            r[6] += e.getDouble("machineTotal"); r[7] += e.getDouble("jvmUser") + e.getDouble("jvmSystem"); r[8]++;
                            break;
                        case "jdk.ContainerCPUThrottling":
                            r[9] = e.getLong("cpuThrottledSlices"); r[10] = ms(e, "cpuThrottledTime");
                            break;
                    }
                }
            }
            System.out.println(Path.of(file).getFileName() + "  (" + window.describe() + ")");
            System.out.println("second    cpuSome cpuFull  steal  running(mean/max)  node%  jvm%  throttled(slices/ms)");
            double[] last = { Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }; // cpuSome, cpuFull, steal, slices, ms
            int[] idx = { 0, 1, 2, 9, 10 };
            for (var en : m.entrySet())
            {
                double[] r = en.getValue();
                String[] d = new String[5];
                for (int i = 0; i < 5; i++)
                {
                    double cur = r[idx[i]];
                    d[i] = Double.isNaN(cur) || Double.isNaN(last[i]) ? "" : String.format("%.0f", cur - last[i]);
                    if (!Double.isNaN(cur)) last[i] = cur;
                }
                System.out.printf("%-9s %7s %7s %6s %8.1f/%-6.0f %6s %5s %10s%n",
                                  LocalTime.ofSecondOfDay(en.getKey() % 86400), d[0], d[1], d[2],
                                  r[5] == 0 ? 0 : r[3] / r[5], r[4],
                                  r[8] == 0 ? "" : String.format("%.0f", 100 * r[6] / r[8]),
                                  r[8] == 0 ? "" : String.format("%.0f", 100 * r[7] / r[8]),
                                  d[3].isEmpty() ? "" : d[3] + "/" + d[4]);
            }
        }
    }

    static double ms(RecordedEvent e, String field)
    {
        return e.getDuration(field).toNanos() / 1e6;
    }
}
