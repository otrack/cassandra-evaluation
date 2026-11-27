import jdk.jfr.consumer.*;
import java.time.*;
import java.util.*;

// DxPhases [--from HH:MM:SS[.sss]] [--to ...] [--skip s] <jfr>... : duration percentiles of the traced transactions'
// coordinator phases ("DX C Client/PreAccept/Propose/Execute/Persist") and of their store tasks' queueing
// ("DX Q <task>"), per node, with TxnIds and stores folded; spans are selected by their start. Splits a client
// latency into protocol phases (round trips, waiting on dependencies in Execute) and local queueing, e.g. over the
// measured window only. Needs -Daccord.debug.distributed.execution=N.
public class DxPhases
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
            System.err.println("usage: DxPhases " + Dx.Window.usage() + " <jfr>...");
            System.exit(2);
        }
        Map<String, Map<String, List<Long>>> byNode = new TreeMap<>();
        List<Dx.Ev> evs = new ArrayList<>();
        Dx.scan(files, evs::add);
        Instant firstClient = null;
        for (Dx.Ev e : evs)
            if (e.tag().startsWith("C Client") && (firstClient == null || e.start().isBefore(firstClient))) firstClient = e.start();
        window.resolve(firstClient);
        for (Dx.Ev e : evs)
        {
            String tag = e.tag();
            if (!tag.startsWith("C ") && !tag.startsWith("Q ")) continue;
            if (!window.contains(e.start())) continue;
            String k = Tags.foldDx("DX " + tag).replace(" <KW>", "").replace(" s<store>", "").substring(3);
            byNode.computeIfAbsent(e.node(), x -> new TreeMap<>()).computeIfAbsent(k, x -> new ArrayList<>()).add(e.durNanos() / 1000);
        }
        System.out.println(window.describe());
        for (var node : byNode.entrySet())
        {
            System.out.println("== " + node.getKey());
            for (var en : node.getValue().entrySet())
            {
                List<Long> v = en.getValue();
                Collections.sort(v);
                System.out.printf("  %-44s n=%6d  p50=%8.1f  p90=%8.1f  p99=%8.1f  max=%8.1f ms%n", en.getKey(), v.size(),
                                  at(v, .5), at(v, .9), at(v, .99), v.get(v.size() - 1) / 1000.0);
            }
        }
    }

    static double at(List<Long> v, double p)
    {
        return v.get(Math.min(v.size() - 1, (int) (v.size() * p))) / 1000.0;
    }
}
