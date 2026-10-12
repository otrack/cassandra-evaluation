import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
// Reads <file> : PlainChain* spans that contain a TxnNamedRead sample on the same thread; duration split by whether any
// sample inside the span touched SSTable reading. (Spans are emitted only when sampled, so durations are length-biased.)
public class Reads {
    static long ns(java.time.Instant i) { return i.getEpochSecond() * 1_000_000_000L + i.getNano(); }
    public static void main(String[] a) throws Exception {
        Map<String, List<long[]>> spans = new HashMap<>(); Map<String, List<long[]>> smp = new HashMap<>(); // thread -> {t, kind(1 read,2 read+sst)}
        try (RecordingFile rf = new RecordingFile(Path.of(a[0]))) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent(); String ty = e.getEventType().getName();
                if (ty.equals("profiler.Span") && e.getString("tag").startsWith("PlainChain")) spans.computeIfAbsent(e.getThread().getJavaName(), k -> new ArrayList<>()).add(new long[]{ns(e.getStartTime()), ns(e.getEndTime())});
                else if ((ty.equals("profiler.WallClockSample") || ty.equals("jdk.ExecutionSample")) && e.getStackTrace() != null) {
                    RecordedThread t = e.getThread("sampledThread"); if (t == null || t.getJavaName() == null || !t.getJavaName().startsWith("AccordExecutor")) continue;
                    boolean read = false, sst = false;
                    for (RecordedFrame f : e.getStackTrace().getFrames()) { String n = f.getMethod().getType().getName();
                        if (n.endsWith("TxnNamedRead$1") || n.endsWith("TxnNamedRead")) read = true;
                        if (n.matches(".*(SSTableReader|BigTableReader|BtiTableReader|RandomAccessReader|ChunkReader|ChunkCache|Rebufferer|BloomFilter|SSTableIterator|RowIndexEntry|PartitionIndex|ChannelProxy).*")) sst = true; }
                    if (read) smp.computeIfAbsent(t.getJavaName(), k -> new ArrayList<>()).add(new long[]{ns(e.getStartTime()), sst ? 2 : 1});
                }
            }
        }
        List<Long> mem = new ArrayList<>(), disk = new ArrayList<>();
        for (var en : spans.entrySet()) {
            List<long[]> ss = new ArrayList<>(smp.getOrDefault(en.getKey(), List.of())); ss.sort(Comparator.comparingLong(x -> x[0]));
            long[] ts = ss.stream().mapToLong(x -> x[0]).toArray();
            for (long[] sp : en.getValue()) {
                int i = Arrays.binarySearch(ts, sp[0]); if (i < 0) i = -i - 1;
                boolean any = false, s2 = false;
                for (int j = i; j < ts.length && ts[j] <= sp[1]; j++) { any = true; if (ss.get(j)[1] == 2) s2 = true; }
                if (any) (s2 ? disk : mem).add((sp[1] - sp[0]) / 1000);
            }
        }
        Collections.sort(mem); Collections.sort(disk);
        System.out.printf("%s: read spans  memtable-only n=%d p50=%dus p90=%dus  |  touching SSTables n=%d p50=%dus p90=%dus%n", Path.of(a[0]).getFileName(),
            mem.size(), mem.isEmpty() ? 0 : mem.get(mem.size() / 2), mem.isEmpty() ? 0 : mem.get(mem.size() * 9 / 10), disk.size(), disk.isEmpty() ? 0 : disk.get(disk.size() / 2), disk.isEmpty() ? 0 : disk.get(disk.size() * 9 / 10));
    }
}
