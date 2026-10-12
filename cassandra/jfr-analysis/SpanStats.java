import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;

// SpanStats <file>... : per-tag span duration stats; store ids are folded into "s<store>" (legacy "[...]" into
// "<store>"), and DX tags into their shape (Tags.foldDx: TxnIds into their kind, keys, peers, ...)
public class SpanStats {
    public static void main(String[] args) throws Exception {
        for (String f : args) {
            Map<String, List<Long>> byTag = new TreeMap<>();
            Map<String, Map<String, Integer>> threadsByTag = new TreeMap<>();
            try (RecordingFile rf = new RecordingFile(Path.of(f))) {
                while (rf.hasMoreEvents()) {
                    RecordedEvent e = rf.readEvent();
                    if (!e.getEventType().getName().equals("profiler.Span")) continue;
                    String raw = e.getString("tag");
                    String tag = Tags.isDx(raw) ? Tags.foldDx(raw)
                                                : raw.replaceAll("\\[[^\\]]*\\|\\d+,\\d+,\\d+\\]", "<store>").replaceAll(" s\\d+$", " s<store>");
                    byTag.computeIfAbsent(tag, k -> new ArrayList<>()).add(e.getDuration().toNanos() / 1000);
                    String tn = e.getThread() == null ? "?" : e.getThread().getJavaName();
                    tn = tn == null ? "?" : tn.replaceAll("\\d+", "N");
                    threadsByTag.computeIfAbsent(tag, k -> new TreeMap<>()).merge(tn, 1, Integer::sum);
                }
            }
            System.out.println("== " + Path.of(f).getFileName());
            System.out.printf("  %-50s %8s %8s %9s %9s %9s %6s %6s  %s%n", "tag", "count", "p50(us)", "p99(us)", "p999(us)", "max(us)", ">100ms", ">1s", "emitting threads");
            for (var en : byTag.entrySet()) {
                List<Long> l = en.getValue(); Collections.sort(l);
                long gt100 = l.stream().filter(x -> x > 100_000).count(), gt1s = l.stream().filter(x -> x > 1_000_000).count();
                System.out.printf("  %-50s %8d %8d %9d %9d %9d %6d %6d  %s%n", en.getKey(), l.size(), l.get(l.size()/2), l.get((int)(l.size()*0.99)), l.get((int)(l.size()*0.999)), l.get(l.size()-1), gt100, gt1s, threadsByTag.get(en.getKey()));
            }
        }
    }
}
