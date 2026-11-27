import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.regex.*;

// CallTree [--from HH:MM:SS[.sss]] [--to ...] [--threads regex] [--anchor regex] [--event cpu|wall|alloc]
//          [--depth N] [--min pct] [--full] <jfr> :
// a call tree (root to leaf) of the samples in a window, weighted by what they measure, optionally only below an
// anchor frame, to see what a code path spends its time (or allocations) on:
//   --event cpu    jdk.ExecutionSample, 1 sample = 1ms of CPU with ctimer interval=1ms (default)
//   --event wall   profiler.WallClockSample, weighted by batch size (includes parked/sleeping threads)
//   --event alloc  allocation samples (cassandra.profiler.alloc, i.e. async-profiler alloc=<bytes>): weighted by the
//                  bytes each sample stands for (tlabSize for jdk.ObjectAllocationInNewTLAB, allocationSize for
//                  jdk.ObjectAllocationOutsideTLAB, weight for the JDK's own jdk.ObjectAllocationSample)
//   --threads      only threads whose name matches (default: all)
//   --anchor       only samples with a frame matching this regex ("Class.method", simple class name), and the tree
//                  starts below its outermost occurrence, e.g. 'LoopTask\.\w+ExclusiveAndUnlock' for the Accord
//                  executor's critical section, or 'ExclusiveExecutor\.runTask' for store tasks
//   --depth        levels to print below the root/anchor (default 12); --min: hide nodes under this % (default 1)
//   --full         fully qualified class names
// Each line: inclusive share, self share, inclusive amount (cores, wall-thread-seconds per second, or MB/s).
public class CallTree
{
    static final class Node
    {
        final String name;
        long total, self;
        final Map<String, Node> children = new HashMap<>();
        Node(String name) { this.name = name; }
        Node child(String n) { return children.computeIfAbsent(n, Node::new); }
    }

    public static void main(String[] args) throws Exception
    {
        Dx.Window window = new Dx.Window();
        String file = null, event = "cpu";
        Pattern threads = null, anchor = null;
        int depth = 12;
        double min = 1;
        boolean full = false;
        for (int i = 0; i < args.length; )
        {
            int n = window.accept(args, i);
            if (n > 0) { i += n; continue; }
            switch (args[i])
            {
                case "--threads": threads = Pattern.compile(args[i + 1]); i += 2; break;
                case "--anchor": anchor = Pattern.compile(args[i + 1]); i += 2; break;
                case "--event": event = args[i + 1]; i += 2; break;
                case "--depth": depth = Integer.parseInt(args[i + 1]); i += 2; break;
                case "--min": min = Double.parseDouble(args[i + 1]); i += 2; break;
                case "--full": full = true; i++; break;
                default: file = args[i++];
            }
        }
        if (file == null || !Set.of("cpu", "wall", "alloc").contains(event))
        {
            System.err.println("usage: CallTree " + Dx.Window.usage() + " [--threads regex] [--anchor regex] [--event cpu|wall|alloc] [--depth N] [--min pct] [--full] <jfr>");
            System.exit(2);
        }

        Node root = new Node(anchor == null ? "<all>" : "<" + anchor.pattern() + ">");
        long matched = 0, all = 0;
        Instant first = null, last = null;
        try (RecordingFile rf = new RecordingFile(Path.of(file)))
        {
            while (rf.hasMoreEvents())
            {
                RecordedEvent e = rf.readEvent();
                long w = weight(e, event);
                if (w <= 0) continue;
                if (!window.contains(e.getStartTime())) continue;
                if (first == null || e.getStartTime().isBefore(first)) first = e.getStartTime();
                if (last == null || e.getStartTime().isAfter(last)) last = e.getStartTime();
                if (threads != null && !threads.matcher(Threads.name(Threads.of(e))).find()) continue;
                all += w;
                RecordedStackTrace st = e.getStackTrace();
                if (st == null) continue;
                List<RecordedFrame> frames = st.getFrames();
                int start = frames.size() - 1; // root
                if (anchor != null)
                {
                    int a = -1;
                    for (int i = frames.size() - 1; i >= 0; i--)
                        if (anchor.matcher(simple(frames.get(i))).find()) { a = i; break; }
                    if (a < 0) continue;
                    start = a - 1;
                }
                matched += w;
                root.total += w;
                Node cur = root;
                for (int i = start; i >= 0; i--)
                {
                    cur = cur.child(full ? fullName(frames.get(i)) : simple(frames.get(i)));
                    cur.total += w;
                }
                cur.self += w;
            }
        }
        if (first == null)
        {
            System.out.println("no " + event + " samples in the window");
            return;
        }
        double secs = Math.max(0.001, Duration.between(first, last).toNanos() / 1e9);
        String unit = event.equals("alloc") ? "MB/s" : event.equals("wall") ? "thr" : "cores";
        double scale = event.equals("alloc") ? 1e6 * secs : event.equals("wall") ? 100.0 * secs /* 10ms wall interval */ : 1000.0 * secs;
        System.out.printf("%s  (%s, %.1fs, event %s%s%s)%n", Path.of(file).getFileName(), window.describe(), secs, event,
                          threads == null ? "" : ", threads " + threads.pattern(), anchor == null ? "" : ", below " + anchor.pattern());
        System.out.printf("matched %.2f %s of %.2f %s (%.1f%%)%n", matched / scale, unit, all / scale, unit, all == 0 ? 0 : 100.0 * matched / all);
        if (event.equals("wall")) System.out.println("(wall: thread-equivalents, assuming the default wall=10ms interval)");
        print(root, root.total, scale, 0, depth, min);
    }

    static long weight(RecordedEvent e, String event)
    {
        String t = e.getEventType().getName();
        switch (event)
        {
            case "cpu": return t.equals("jdk.ExecutionSample") ? 1 : 0;
            case "wall": return t.equals("profiler.WallClockSample") ? e.getLong("samples") : 0;
            default:
                switch (t)
                {
                    case "jdk.ObjectAllocationInNewTLAB": return Math.max(e.getLong("tlabSize"), e.getLong("allocationSize"));
                    case "jdk.ObjectAllocationOutsideTLAB": return e.getLong("allocationSize");
                    case "jdk.ObjectAllocationSample": return e.getLong("weight");
                    default: return 0;
                }
        }
    }

    static String simple(RecordedFrame f)
    {
        String cls = f.getMethod().getType().getName();
        int dot = cls.lastIndexOf('.');
        String s = (dot >= 0 ? cls.substring(dot + 1) : cls) + "." + f.getMethod().getName();
        return s.replaceAll("\\$\\$Lambda[./]0x[0-9a-f]+", "\\$\\$Lambda");
    }

    static String fullName(RecordedFrame f)
    {
        return (f.getMethod().getType().getName() + "." + f.getMethod().getName()).replaceAll("\\$\\$Lambda[./]0x[0-9a-f]+", "\\$\\$Lambda");
    }

    static void print(Node n, long rootTotal, double scale, int level, int depth, double min)
    {
        if (rootTotal == 0) return;
        System.out.printf("%5.1f%% %5.1f%% %7.2f  %s%s%n", 100.0 * n.total / rootTotal, 100.0 * n.self / rootTotal, n.total / scale,
                          "  ".repeat(level), n.name);
        if (level >= depth) return;
        List<Node> kids = new ArrayList<>(n.children.values());
        kids.sort((a, b) -> Long.compare(b.total, a.total));
        long hidden = 0;
        for (Node k : kids)
        {
            if (100.0 * k.total / rootTotal < min) { hidden += k.total; continue; }
            print(k, rootTotal, scale, level + 1, depth, min);
        }
        if (hidden > 0 && 100.0 * hidden / rootTotal >= 0.1)
            System.out.printf("%5.1f%%        %7.2f  %s(others)%n", 100.0 * hidden / rootTotal, hidden / scale, "  ".repeat(level + 1));
    }
}
