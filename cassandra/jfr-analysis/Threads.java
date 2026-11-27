import jdk.jfr.consumer.*;
import java.util.*;

/**
 * Shared classification of the threads in a capture, for the CPU tools (WindowCpu, WorkRate, JitCpu, ...).
 *
 * <p>JIT compiler threads started on demand (-XX:+UseDynamicNumberOfCompilerThreads) have no Java name in the
 * recording ("[tid=N]"), so JIT and GC work is recognised by stack: CompileBroker::compiler_thread_loop, split into
 * C1 and C2 by the compiler's entry point, and the ZGC/G1 worker frames.
 */
final class Threads
{
    private Threads() {}

    /** the thread's Java name, or its OS name, or "?" */
    static String name(RecordedThread th)
    {
        if (th == null) return "?";
        if (th.getJavaName() != null) return th.getJavaName();
        return th.getOSName() != null ? th.getOSName() : "?";
    }

    /** the thread that a sample event is of: sampledThread for profiler samples, else the event thread */
    static RecordedThread of(RecordedEvent e)
    {
        return e.hasField("sampledThread") ? e.getThread("sampledThread") : e.getThread();
    }

    /** a thread name with its numbers folded, e.g. "AccordExecutor[N,N]" */
    static String fold(String name)
    {
        return name.replaceAll("\\d+", "N");
    }

    /**
     * the category of a CPU sample: C1/C2/JIT (compiler threads), GC, AccExec, MsgLoop, NTR, Shared, Journal,
     * VMThr, other
     */
    static String category(RecordedEvent e)
    {
        RecordedStackTrace st = e.getStackTrace();
        if (st != null)
        {
            boolean compiler = false;
            for (RecordedFrame f : st.getFrames())
            {
                String m = f.getMethod().getType().getName() + "::" + f.getMethod().getName();
                if (m.contains("C2Compiler::compile_method")) return "C2";
                if (m.contains("Compiler::compile_method")) return "C1";
                if (m.contains("compiler_thread_loop")) compiler = true;
                if (m.contains("ZDriver") || m.contains("ZWorker") || m.contains("ZDirector") || m.contains("WorkerThread::run")
                    || m.contains("ConcurrentGCThread")) return "GC";
            }
            if (compiler) return "JIT";
        }
        String t = name(of(e));
        if (t.startsWith("AccordExecutor")) return "AccExec";
        if (t.startsWith("Messaging-EventLoop")) return "MsgLoop";
        if (t.contains("CompilerThread") || t.startsWith("C2 ") || t.startsWith("C1 ")) return "JIT";
        if (t.startsWith("Z") || t.startsWith("GC") || t.startsWith("G1")) return "GC";
        if (t.startsWith("Native-Transport") || t.contains("epollEventLoop") || t.contains("nioEventLoop")) return "NTR";
        if (t.startsWith("SharedPool")) return "Shared";
        if (t.contains("Journal")) return "Journal";
        if (t.startsWith("VM Thread")) return "VMThr";
        return "other";
    }

    static boolean isJit(String category)
    {
        return category.equals("C1") || category.equals("C2") || category.equals("JIT");
    }

    /** whether the stack contains a frame whose "Class.method" (simple class name) ends with the suffix */
    static boolean hasFrame(RecordedStackTrace st, String suffix)
    {
        if (st == null) return false;
        for (RecordedFrame f : st.getFrames())
            if ((f.getMethod().getType().getName() + "." + f.getMethod().getName()).endsWith(suffix))
                return true;
        return false;
    }
}
