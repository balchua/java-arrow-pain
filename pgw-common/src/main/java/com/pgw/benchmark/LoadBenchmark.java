package com.pgw.benchmark;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Records timing measurements and memory usage for the XML-to-Arrow loading
 * pipeline.
 * <p>
 * Tracks the duration of each phase (parsing, writing), heap and off-heap
 * memory consumption, and provides a formatted report.
 * </p>
 */
public final class LoadBenchmark {

    private final String label;
    private final Map<String, Duration> phases = new LinkedHashMap<>();
    private long xmlFileSizeBytes;
    private long arrowFileSizeBytes;
    private long totalRows;
    private long messageRows;
    private long remittanceRows;

    // ── Heap memory (Java heap) ─────────────────────────────────────────────
    private long heapUsedBeforeBytes;
    private long heapUsedAfterBytes;
    private long heapMaxBytes;

    // ── Off-heap memory (Arrow native) ──────────────────────────────────────
    private long offHeapAllocatedBytes;
    private long offHeapPeakBytes;
    private long offHeapLimitBytes;

    // ── Streaming Arrow peak (explicit per-batch sample) ─────────────────────
    private long offHeapStreamingPeakBytes;

    // ── DuckDB memory budget (proxy for pod impact) ───────────────────────────
    private long duckDbMemoryLimitBytes;

    // ── Validation ──────────────────────────────────────────────────────────
    private boolean validationPassed;
    private long validationRemittances;
    private long validationTransactions;
    private int validationErrors;

    /**
     * Creates a new benchmark with the given label.
     *
     * @param label descriptive label for this benchmark run
     */
    public LoadBenchmark(String label) {
        this.label = label;
    }

    /**
     * Records a phase duration.
     *
     * @param phaseName the name of the phase
     * @param duration  the elapsed duration
     */
    public void recordPhase(String phaseName, Duration duration) {
        phases.put(phaseName, duration);
    }

    public void setXmlFileSizeBytes(long bytes) {
        this.xmlFileSizeBytes = bytes;
    }

    public void setArrowFileSizeBytes(long bytes) {
        this.arrowFileSizeBytes = bytes;
    }

    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }

    public void setMessageRows(long messageRows) {
        this.messageRows = messageRows;
    }

    public void setRemittanceRows(long remittanceRows) {
        this.remittanceRows = remittanceRows;
    }

    // ── Heap memory setters ─────────────────────────────────────────────────

    public void setHeapUsedBeforeBytes(long bytes) {
        this.heapUsedBeforeBytes = bytes;
    }

    public void setHeapUsedAfterBytes(long bytes) {
        this.heapUsedAfterBytes = bytes;
    }

    public void setHeapMaxBytes(long bytes) {
        this.heapMaxBytes = bytes;
    }

    // ── Off-heap memory setters ─────────────────────────────────────────────

    public void setOffHeapAllocatedBytes(long bytes) {
        this.offHeapAllocatedBytes = bytes;
    }

    public void setOffHeapPeakBytes(long bytes) {
        this.offHeapPeakBytes = bytes;
    }

    public void setOffHeapLimitBytes(long bytes) {
        this.offHeapLimitBytes = bytes;
    }

    public long getOffHeapStreamingPeakBytes() {
        return offHeapStreamingPeakBytes;
    }

    /**
     * Called once per batch flush during streaming parse.
     * Updates {@code offHeapStreamingPeakBytes} if {@code currentBytes} is larger.
     * Thread-safe (called from the parse loop, not concurrently, but defensive).
     */
    public synchronized void sampleOffHeap(long currentBytes) {
        if (currentBytes > offHeapStreamingPeakBytes) {
            offHeapStreamingPeakBytes = currentBytes;
        }
    }

    /**
     * Sets the DuckDB memory limit in bytes (parsed from the SET memory_limit string).
     * Used as a proxy for DuckDB's pod footprint contribution.
     */
    public void setDuckDbMemoryLimitBytes(long bytes) {
        this.duckDbMemoryLimitBytes = bytes;
    }

    /**
     * Parses DuckDB's SET memory_limit value (e.g. "1GB", "512MB", "2048MB") to bytes.
     * Returns 0 if the string cannot be parsed.
     * Supported suffixes (case-insensitive): KB, MB, GB, TB.
     */
    public static long parseDuckDbMemoryLimit(String limitStr) {
        if (limitStr == null || limitStr.isBlank()) return 0L;
        String s = limitStr.trim().toUpperCase();
        try {
            if (s.endsWith("TB")) return (long) (Double.parseDouble(s.replace("TB", "").trim())
                    * 1024L * 1024L * 1024L * 1024L);
            if (s.endsWith("GB")) return (long) (Double.parseDouble(s.replace("GB", "").trim())
                    * 1024L * 1024L * 1024L);
            if (s.endsWith("MB")) return (long) (Double.parseDouble(s.replace("MB", "").trim())
                    * 1024L * 1024L);
            if (s.endsWith("KB")) return (long) (Double.parseDouble(s.replace("KB", "").trim())
                    * 1024L);
        } catch (NumberFormatException e) {
            // fall through to return 0
        }
        return 0L;
    }

    /**
     * Records control sum validation results.
     */
    public void setValidationResult(boolean passed, long remittances,
            long transactions, int errors) {
        this.validationPassed = passed;
        this.validationRemittances = remittances;
        this.validationTransactions = transactions;
        this.validationErrors = errors;
    }

    /**
     * Captures current heap usage snapshot.
     *
     * @return used heap in bytes (totalMemory − freeMemory)
     */
    public static long captureHeapUsed() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    /**
     * Returns a formatted report string.
     */
    public String toReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n╔══════════════════════════════════════════════════════════════╗\n");
        sb.append(String.format("║  BENCHMARK: %-48s ║%n", label));
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append(String.format("║  XML File Size    : %,15d bytes (%,.1f MB)       ║%n",
                xmlFileSizeBytes, xmlFileSizeBytes / (1024.0 * 1024.0)));
        if (arrowFileSizeBytes > 0) {
            sb.append(String.format("║  Arrow File Size  : %,15d bytes (%,.1f MB)       ║%n",
                    arrowFileSizeBytes, arrowFileSizeBytes / (1024.0 * 1024.0)));
        }
        sb.append(String.format("║  Message rows     : %,15d                       ║%n", messageRows));
        sb.append(String.format("║  Remittance rows  : %,15d                       ║%n", remittanceRows));
        sb.append(String.format("║  Transaction rows : %,15d                       ║%n", totalRows));
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");

        for (Map.Entry<String, Duration> entry : phases.entrySet()) {
            Duration d = entry.getValue();
            double seconds = d.toMillis() / 1000.0;
            sb.append(String.format("║  %-18s: %,10d ms  (%,.2f s)              ║%n",
                    entry.getKey(), d.toMillis(), seconds));
        }

        // ── Throughput ──────────────────────────────────────────────────────
        Duration parseDuration = phases.get("XML→Arrow Parse");
        if (parseDuration != null && parseDuration.toMillis() > 0) {
            double parseSeconds = parseDuration.toMillis() / 1000.0;
            double rowsPerSecond = totalRows / parseSeconds;
            double mbPerSecond = (xmlFileSizeBytes / (1024.0 * 1024.0)) / parseSeconds;
            sb.append("╠══════════════════════════════════════════════════════════════╣\n");
            sb.append(String.format("║  Parse Throughput : %,.0f rows/sec                    ║%n",
                    rowsPerSecond));
            sb.append(String.format("║  Parse Throughput : %,.2f MB/sec                       ║%n",
                    mbPerSecond));
        }

        // ── DuckDB registration ─────────────────────────────────────────────
        Duration duckdbDuration = phases.get("DuckDB Registration");
        if (duckdbDuration != null) {
            sb.append("╠══════════════════════════════════════════════════════════════╣\n");
            sb.append("║  DUCKDB REGISTRATION                                       ║\n");
            sb.append(String.format("║  Appender load time: %,10d ms                          ║%n",
                    duckdbDuration.toMillis()));
        }

        // ── Validation ──────────────────────────────────────────────────────
        Duration valDuration = phases.get("SQL Validation");
        if (valDuration == null) {
            valDuration = phases.get("Validation");
        }
        if (valDuration != null) {
            sb.append("╠══════════════════════════════════════════════════════════════╣\n");
            sb.append("║  VALIDATION                                                ║\n");
            sb.append(String.format("║  Result           : %-40s ║%n",
                    validationPassed ? "✓ PASSED" : "✗ FAILED (" + validationErrors + " errors)"));
            sb.append(String.format("║  Remittances      : %,15d checked                  ║%n",
                    validationRemittances));
            sb.append(String.format("║  Transactions     : %,15d scanned                  ║%n",
                    validationTransactions));
            double valMs = valDuration.toNanos() / 1_000_000.0;
            sb.append(String.format("║  Validation time  : %13.3f ms                        ║%n",
                    valMs));
            if (validationTransactions > 0 && valMs > 0) {
                double rowsPerMs = validationTransactions / valMs;
                sb.append(String.format("║  Scan throughput  : %,.0f rows/ms  (%,.0f M rows/sec)     ║%n",
                        rowsPerMs, rowsPerMs * 1000.0 / 1_000_000.0));
            }
        }

        // ── Legacy CtrlSum Validate phase (kept for backward compatibility) ──
        Duration ctrlSumDuration = phases.get("CtrlSum Validate");
        if (ctrlSumDuration != null) {
            sb.append("╠══════════════════════════════════════════════════════════════╣\n");
            sb.append("║  CONTROL SUM VALIDATION                                    ║\n");
            sb.append(String.format("║  Result           : %-40s ║%n",
                    validationPassed ? "✓ PASSED" : "✗ FAILED (" + validationErrors + " errors)"));
            sb.append(String.format("║  Remittances      : %,15d checked                  ║%n",
                    validationRemittances));
            sb.append(String.format("║  Transactions     : %,15d scanned                  ║%n",
                    validationTransactions));
            double valMs = ctrlSumDuration.toNanos() / 1_000_000.0;
            sb.append(String.format("║  Validation time  : %13.3f ms                        ║%n",
                    valMs));
        }

        // ── Memory usage ────────────────────────────────────────────────────
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append("║  MEMORY USAGE                                              ║\n");
        sb.append(String.format("║  Heap before      : %,15d bytes (%,.1f MB)       ║%n",
                heapUsedBeforeBytes, heapUsedBeforeBytes / (1024.0 * 1024.0)));
        sb.append(String.format("║  Heap after       : %,15d bytes (%,.1f MB)       ║%n",
                heapUsedAfterBytes, heapUsedAfterBytes / (1024.0 * 1024.0)));
        sb.append(String.format("║  Heap delta       : %,15d bytes (%,.1f MB)       ║%n",
                heapUsedAfterBytes - heapUsedBeforeBytes,
                (heapUsedAfterBytes - heapUsedBeforeBytes) / (1024.0 * 1024.0)));
        sb.append(String.format("║  Heap max (-Xmx)  : %,15d bytes (%,.1f MB)       ║%n",
                heapMaxBytes, heapMaxBytes / (1024.0 * 1024.0)));
        sb.append("║  ──────────────────────────────────────────────────────────║\n");
        sb.append(String.format("║  Off-heap alloc'd : %,15d bytes (%,.1f MB)       ║%n",
                offHeapAllocatedBytes, offHeapAllocatedBytes / (1024.0 * 1024.0)));
        sb.append(String.format(
                "║  Off-heap peak    : %,15d bytes (%,.1f MB)  [Arrow allocator lifetime HWM]  ║%n",
                offHeapPeakBytes, offHeapPeakBytes / (1024.0 * 1024.0)));
        String streamPeakNote = offHeapStreamingPeakBytes == 0
                ? "(not sampled — legacy mode)"
                : String.format("%,.1f MB", offHeapStreamingPeakBytes / (1024.0 * 1024.0));
        sb.append(String.format(
                "║  Off-heap stream  : %,15d bytes (%s)  [max sample during parse loop]  ║%n",
                offHeapStreamingPeakBytes, streamPeakNote));
        sb.append(String.format("║  Off-heap limit   : %,15d bytes (%,.1f MB)       ║%n",
                offHeapLimitBytes, offHeapLimitBytes / (1024.0 * 1024.0)));
        sb.append("║  ──────────────────────────────────────────────────────────║\n");
        String duckDbNote = duckDbMemoryLimitBytes == 0 ? "(not set)" :
                String.format("%,.1f MB", duckDbMemoryLimitBytes / (1024.0 * 1024.0));
        sb.append(String.format(
                "║  DuckDB limit     : %,15d bytes (%s)  [estimate — SET memory_limit]   ║%n",
                duckDbMemoryLimitBytes, duckDbNote));
        sb.append("║  ──────────────────────────────────────────────────────────║\n");
        long effectiveArrowPeak = Math.max(offHeapPeakBytes, offHeapStreamingPeakBytes);
        long totalPodImpact = heapUsedAfterBytes + effectiveArrowPeak + duckDbMemoryLimitBytes;
        sb.append(String.format(
                "║  Effective Arrow  : %,15d bytes (%,.1f MB)  [max(offHeapPeak, streamPeak)]  ║%n",
                effectiveArrowPeak, effectiveArrowPeak / (1024.0 * 1024.0)));
        sb.append(String.format(
                "║  Total Pod Impact : %,15d bytes (%,.1f MB)  [Heap + Arrow + DuckDB *est*]   ║%n",
                totalPodImpact, totalPodImpact / (1024.0 * 1024.0)));

        sb.append("╚══════════════════════════════════════════════════════════════╝\n");
        return sb.toString();
    }
}
