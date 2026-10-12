import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;

// QueueByStore <file> : queue time (Queued spans) by the command store / kind of the task that was queued.
//
// Current tags ("Queued Accept s24") carry the task's kind and store directly. Legacy recordings only have
// "AccordTaskQueued", so each queued span is attributed to the run span that starts on the same thread within
// 100us of its end; only a few percent can be matched that way, so treat legacy output as a sample.
public class QueueByStore
{
    static long ns(java.time.Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    record S(long s, long e, String tag) {}

    public static void main(String[] a) throws Exception
    {
        Map<String, List<S>> byThread = new HashMap<>();
        try (RecordingFile rf = new RecordingFile(Path.of(a[0])))
        {
            while (rf.hasMoreEvents())
            {
                RecordedEvent e = rf.readEvent();
                if (!e.getEventType().getName().equals("profiler.Span")) continue;
                String tag = e.getString("tag");
                if (Tags.isDx(tag) || Tags.isCriticalSection(tag) || Tags.isHead(tag) || Tags.isParkedWithWork(tag)) continue;
                byThread.computeIfAbsent(e.getThread().getJavaName(), k -> new ArrayList<>()).add(new S(ns(e.getStartTime()), ns(e.getEndTime()), tag));
            }
        }

        Map<String, List<Long>> byStore = new HashMap<>(), byKind = new TreeMap<>();
        long matched = 0, total = 0;
        boolean legacy = false;
        for (List<S> l : byThread.values())
        {
            List<S> runs = new ArrayList<>(), qs = new ArrayList<>();
            for (S s : l) (Tags.isQueued(s.tag()) ? qs : runs).add(s);
            runs.sort(Comparator.comparingLong(S::s));
            long[] rs = runs.stream().mapToLong(S::s).toArray();
            for (S x : qs)
            {
                total++;
                String attributed = x.tag();
                if (Tags.label(attributed) == null)
                {
                    legacy = true;
                    int i = Arrays.binarySearch(rs, x.e()); if (i < 0) i = -i - 1;
                    S best = null; long bd = Long.MAX_VALUE;
                    for (int j = Math.max(0, i - 1); j < Math.min(rs.length, i + 2); j++) { long d = Math.abs(rs[j] - x.e()); if (d < bd) { bd = d; best = runs.get(j); } }
                    if (best == null || bd > 100_000) continue;
                    attributed = best.tag();
                }
                matched++;
                long us = (x.e() - x.s()) / 1000;
                String store = Tags.store(attributed);
                String kind = Tags.kind(attributed);
                byStore.computeIfAbsent(store == null ? "(none)" : (Tags.isStoreTask(attributed) ? "store " : "off-store, for store ") + store, k -> new ArrayList<>()).add(us);
                byKind.computeIfAbsent(kind == null ? "?" : kind, k -> new ArrayList<>()).add(us);
            }
        }

        System.out.printf("%s: %d queued spans%s%n", Path.of(a[0]).getFileName(), total,
                          legacy ? String.format(" (legacy tags: %d attributed by pairing with the next run span)", matched) : "");
        System.out.printf("  %-34s %8s %9s %9s %9s%n", "queued task", "n", "p50(us)", "p90(us)", "p99(us)");
        // busiest stores first, then the rest folded together
        List<Map.Entry<String, List<Long>>> stores = new ArrayList<>(byStore.entrySet());
        stores.sort((x, y) -> Integer.compare(y.getValue().size(), x.getValue().size()));
        List<Long> rest = new ArrayList<>();
        for (int i = 0; i < stores.size(); i++)
        {
            if (i < 5) print(stores.get(i).getKey(), stores.get(i).getValue());
            else rest.addAll(stores.get(i).getValue());
        }
        if (!rest.isEmpty()) print("(all other stores)", rest);
        System.out.println("  by kind of work:");
        byKind.entrySet().stream().sorted((x, y) -> Integer.compare(y.getValue().size(), x.getValue().size())).limit(12)
              .forEach(x -> print(x.getKey(), x.getValue()));
    }

    static void print(String key, List<Long> v)
    {
        Collections.sort(v);
        System.out.printf("  %-34s %8d %9d %9d %9d%n", key.length() > 34 ? key.substring(0, 34) : key, v.size(),
                          v.get(v.size() / 2), v.get((int) (v.size() * 0.9)), v.get((int) (v.size() * 0.99)));
    }
}
