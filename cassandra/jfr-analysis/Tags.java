import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses profiler.Span tags emitted by Cassandra's DebugExecution, in both formats:
 * <ul>
 *   <li>legacy: "AccordTaskQueued", store task runs "[table|tid|&lt;store&gt;,&lt;executor&gt;,&lt;node&gt;]",
 *       other runs by class ("PlainChain", "IOTaskLoad", ...)</li>
 *   <li>current (TaskLabels): "Queued &lt;label&gt;", "Head &lt;label&gt;", "&lt;label&gt;", where label is
 *       "&lt;kind&gt;[ s&lt;store&gt;]", e.g. "Accept s24", "PlainChain s24", "PlainRunnable"</li>
 * </ul>
 * plus "AccordExecutorCriticalSection" and "AccordExecutorParkedWithWork" in both.
 *
 * <p>DebugDistributedExecution's per-transaction "DX ..." spans (see Dx) are not executor work, even the "DX Q"/"DX T"
 * spans that mirror a traced transaction's store tasks: {@link #isDx} identifies them, the other predicates exclude
 * them, and {@link #foldDx} reduces them to their shape for aggregate statistics.
 */
final class Tags
{
    private static final Pattern LEGACY_STORE = Pattern.compile("^\\[.*\\|(\\d+),\\d+,\\d+\\]$");
    private static final Pattern LABEL = Pattern.compile("^(.+?)(?: s(\\d+))?$");

    private static final Pattern TXN = Pattern.compile("\\[\\d+,\\d+,\\d+\\((\\w+)\\),\\d+\\]");

    static boolean isDx(String tag) { return tag.startsWith("DX "); }
    static boolean isQueued(String tag) { return tag.equals("AccordTaskQueued") || tag.startsWith("Queued "); }
    static boolean isHead(String tag) { return tag.startsWith("Head "); }
    static boolean isCriticalSection(String tag) { return tag.equals("AccordExecutorCriticalSection"); }
    static boolean isParkedWithWork(String tag) { return tag.equals("AccordExecutorParkedWithWork"); }
    static boolean isRun(String tag) { return !isDx(tag) && !isQueued(tag) && !isHead(tag) && !isCriticalSection(tag) && !isParkedWithWork(tag); }

    /**
     * a DX tag reduced to its shape: TxnIds to "<KW>" etc. (their kind), keys to "k<key>", stores to "s<store>",
     * peers to "n<peer>", dependency counts to "deps=<n>", and embedded contexts ("NotifyWaitingOn{...}") to "{..}";
     * e.g. "DX T PreAccept <KW> s<store>" or "DX K BlockedOn <KW> k<key> s<store> Stable HasStableDeps by <KW>"
     */
    static String foldDx(String tag)
    {
        return TXN.matcher(tag).replaceAll("<$1>")
                  .replaceAll("\\{[^ ]*\\}", "{..}")
                  .replaceAll(" k\\S+", " k<key>")
                  .replaceAll(" s\\d+", " s<store>")
                  .replaceAll(" n\\d+", " n<peer>")
                  .replaceAll("deps=\\d+", "deps=<n>");
    }

    /** the label without any Queued/Head prefix, or null for the legacy AccordTaskQueued tag */
    static String label(String tag)
    {
        if (tag.equals("AccordTaskQueued")) return null;
        if (tag.startsWith("Queued ")) return tag.substring(7);
        if (tag.startsWith("Head ")) return tag.substring(5);
        return tag;
    }

    /** the kind of work: the request (store tasks) or task class; "store" for legacy store task runs, null if unknown */
    static String kind(String tag)
    {
        String label = label(tag);
        if (label == null) return null;
        if (LEGACY_STORE.matcher(label).matches()) return "store";
        Matcher m = LABEL.matcher(label);
        return m.matches() ? m.group(1) : label;
    }

    /** the command store id the work is for, or null */
    static String store(String tag)
    {
        String label = label(tag);
        if (label == null) return null;
        Matcher legacy = LEGACY_STORE.matcher(label);
        if (legacy.matches()) return legacy.group(1);
        Matcher m = LABEL.matcher(label);
        return m.matches() ? m.group(2) : null;
    }

    /** whether this is work run exclusively on its command store (a SafeTask), rather than off-store work for it */
    static boolean isStoreTask(String tag)
    {
        String kind = kind(tag), store = store(tag);
        if (store == null || kind == null) return false;
        return !(kind.startsWith("Plain") || kind.startsWith("IOTask") || kind.startsWith("ExclusiveExecutor"));
    }
}
