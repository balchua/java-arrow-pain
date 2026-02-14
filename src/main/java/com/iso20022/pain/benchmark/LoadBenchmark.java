package com.iso20022.pain.benchmark;

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

        // ── Validation ──────────────────────────────────────────────────────
        Duration valDuration = phases.get("CtrlSum Validate");
        if (valDuration != null) {
            sb.append("╠══════════════════════════════════════════════════════════════╣\n");
            sb.append("║  CONTROL SUM VALIDATION                                    ║\n");
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
        sb.append(String.format("║  Off-heap peak    : %,15d bytes (%,.1f MB)       ║%n",
                offHeapPeakBytes, offHeapPeakBytes / (1024.0 * 1024.0)));
        sb.append(String.format("║  Off-heap limit   : %,15d bytes (%,.1f MB)       ║%n",
                offHeapLimitBytes, offHeapLimitBytes / (1024.0 * 1024.0)));
        sb.append("║  ──────────────────────────────────────────────────────────║\n");
        long combinedPeak = heapUsedAfterBytes + offHeapPeakBytes;
        sb.append(String.format("║  Combined peak    : %,15d bytes (%,.1f MB)       ║%n",
                combinedPeak, combinedPeak / (1024.0 * 1024.0)));

        sb.append("╚══════════════════════════════════════════════════════════════╝\n");
        return sb.toString();
    }
}
