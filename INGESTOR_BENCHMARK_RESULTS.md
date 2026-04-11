# PGW — Ingestion Benchmark Results

Ingestion modules: `pgw-common` + `pgw-ingestor` + `pgw-ingestor-pure-arrow`

See also [VALIDATOR_BENCHMARK_RESULTS.md](VALIDATOR_BENCHMARK_RESULTS.md) for validator benchmark results.

**Environment:** Java 25 (Temurin 25.0.2), Maven 3.9, `-Xmx4g`,
`--add-opens=java.base/java.nio=ALL-UNNAMED`

**No extensions required.** Arrow export and import use DuckDB's built-in C Data Interface
(`DuckDBResultSet.arrowExportStream` / `registerArrowStream`) — works in air-gapped environments.

---

## How to Run All Tests

```bash
# No extension installation needed.

# Full suite — all 6 modules
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test

# Standardized run with timestamped log saved to test-results/
./run_validation_tests.sh
```

---

# `pgw-ingestor` — XML Parse + DuckDB INSERT + Arrow Export Tests

**Responsibility:** StAX XML parsing → Arrow IPC batches → DuckDB live INSERT →
`ArrowIpc.export()` → `.arrow` files on disk. No domain knowledge.

**Path:** XML → StAX streaming parse → `StreamingBatchConsumer` (INSERT per batch) → DuckDB →
`ArrowIpc.export()` (C Data Interface, no extension) → `.arrow` files

## How to Run `pgw-ingestor` Tests

```bash
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor

# Ingestion benchmark only
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor -Dtest=IngestionBenchmarkTest
```

## `pgw-ingestor` Test Classes

| Test Class | What It Tests | Tests |
|-----------|--------------|------:|
| `ParsePipelineTest` | StAX parser correctness: row counts, field values, edge cases | 4 |
| `StreamingPipelineTest` | Memory footprint; DuckDB row counts; `ArrowIpc.export` + `ArrowIpc.load` round-trip | 4 |
| `MemoryLeakVerificationTest` | 50-iteration streaming parse stress test — zero bytes leaked | 3 |
| `IngestionBenchmarkTest` | XML → DuckDB INSERT → Arrow export benchmark for Types A–J | 1 |
| **Total** | | **12** |

## `pgw-ingestor` Test Results

```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0  -- ParsePipelineTest
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0  -- StreamingPipelineTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0  -- MemoryLeakVerificationTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- IngestionBenchmarkTest

pgw-ingestor  :  12 tests — BUILD SUCCESS
```

## `pgw-ingestor` — XML → DuckDB INSERT → Arrow Export Benchmark (`IngestionBenchmarkTest`)

Results from actual test run (2026-04-11, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║       Ingestion Benchmark — XML → StAX Parse → DuckDB INSERT → ArrowIpc.export()                       ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  Type    ║ XML (MB) ║ Parse+Ins ms ║ Export ms  ║ Peak Off-Heap  ║ Arrow (MB) ║  Tx Rows  ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  Type A  ║   734.4  ║      16,196  ║       612  ║   31,309,824   ║    294.64 ║ 1,000,000 ║
║  Type B  ║   734.3  ║      14,512  ║       557  ║   31,309,824   ║    294.54 ║ 1,000,000 ║
║  Type C  ║ 1,295.1  ║      30,913  ║       914  ║   52,101,120   ║    494.90 ║ 1,000,000 ║
║  Type D  ║     0.1  ║          12  ║         5  ║    1,785,856   ║      0.06 ║       200 ║
║  Type E  ║     0.1  ║          13  ║         4  ║    1,785,856   ║      0.06 ║       200 ║
║  Type F  ║ 1,469.8  ║      30,588  ║     1,091  ║   31,309,824   ║    590.34 ║ 2,000,000 ║
║  Type G  ║ 2,940.7  ║      62,249  ║     2,354  ║   31,309,824   ║  1,181.73 ║ 4,000,000 ║
║  Type H  ║     1.5  ║          31  ║         8  ║    2,080,768   ║      0.59 ║     2,000 ║
║  Type I  ║     1.5  ║          33  ║         8  ║    2,080,768   ║      0.59 ║     2,000 ║
║  Type J  ║     0.0  ║           6  ║         4  ║    1,785,856   ║      0.01 ║         1 ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  TOTAL   ║ 7,177.5  ║     154,553  ║     5,557  ║   52,101,120   ║  2,857.46 ║ 9,004,401 ║
╚══════════╩══════════╩══════════════╩════════════╩════════════════╩════════════╩═══════════╝

  XML (MB)       = source XML file size on disk
  Parse+Ins ms   = StAX streaming parse + StreamingBatchConsumer INSERT into DuckDB
  Export ms      = ArrowIpc.export() for all 3 tables (C Data Interface, no extension)
  Peak Off-Heap  = peak Arrow allocator off-heap bytes during parse+insert phase
                   DuckDB path streams each batch through DuckDB and releases it → BOUNDED at ~31 MB
  Arrow (MB)     = combined size of the 3 exported .arrow files on disk
  Tx Rows        = transaction rows ingested
```

**Key findings — DuckDB pipeline:**
- **Peak off-heap stays bounded at ~31 MB** for all single-remittance types (A, B, D, E, F, G, H, I, J)
  because `StreamingBatchConsumer` feeds each 65k-row batch to DuckDB via `registerArrowStream` and
  immediately releases the Arrow buffer — only 1 batch lives in Arrow memory at any time
- Type C peaks at ~52 MB because 1M separate remittance batches are processed (one batch per PmtInf)
- Type G (4M rows) scales linearly with Type F: 62,249 ms vs 30,588 ms
- DuckDB export is fast (< 2.4 s even for 4M rows) since it reads from DuckDB, not XML
- **Type H** (10 × 200 = 2,000 tx): 31 ms parse, 8 ms export — multi-remittance correctness baseline
- **Type I** (5 × 400 = 2,000 tx): 33 ms parse, 8 ms export — same total rows as H, different grouping
- **Type J** (1 × 1 = 1 tx): 6 ms parse, 4 ms export — **unitary baseline**, minimal XML payload

## `pgw-ingestor` — Memory Leak Verification (`MemoryLeakVerificationTest`)

| Scenario | Iterations | Bytes Leaked |
|----------|----------:|------------:|
| Type D (valid, streaming) | 50 | **0** |
| Type E (invalid CtrlSum, streaming) | 50 | **0** |
| Type D (short) | 3 | **0** |

✅ **Zero bytes leaked across all 103 iterations.**

---

# `pgw-ingestor-pure-arrow` — Pure Arrow Pipeline Tests (no DuckDB at ingest)

**Responsibility:** StAX XML parsing → `PureArrowBatchConsumer` (VectorUnloader per batch) →
`PureArrowInMemoryStore` → `ArrowStreamWriter` → `.arrow` files. No DuckDB during ingest.

**⚠ Memory model difference:** Unlike the DuckDB pipeline, the pure-Arrow pipeline **accumulates
ALL `ArrowRecordBatch` objects** in `PureArrowInMemoryStore` until `store.close()` is called.
This means peak off-heap grows proportionally to the total number of rows ingested, not just 1 batch.

## How to Run `pgw-ingestor-pure-arrow` Tests

```bash
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor-pure-arrow

# Comparison benchmark only
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor-pure-arrow -Dtest=PipelineComparisonBenchmarkTest
```

## `pgw-ingestor-pure-arrow` Test Classes

| Test Class | What It Tests | Tests |
|-----------|--------------|------:|
| `PureArrowParsePipelineTest` | Parse correctness: row counts, IPC file creation, ArrowStreamReader round-trip | 5 |
| `PureArrowStreamingPipelineTest` | Memory footprint, row counts, multi-ingest allocator sharing | 3 |
| `PureArrowMemoryLeakVerificationTest` | 50-iteration zero-leak test for Types D + E | 2 |
| `PureArrowIngestionBenchmarkTest` | XML → Arrow IPC benchmark for Types A–J (no DuckDB) | 1 |
| `PipelineComparisonBenchmarkTest` | DuckDB pipeline vs Pure-Arrow pipeline side-by-side (Types A–J) | 1 |
| **Total** | | **12** |

## `pgw-ingestor-pure-arrow` Test Results

```
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0  -- PureArrowParsePipelineTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0  -- PureArrowStreamingPipelineTest
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0  -- PureArrowMemoryLeakVerificationTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- PureArrowIngestionBenchmarkTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- PipelineComparisonBenchmarkTest

pgw-ingestor-pure-arrow  :  12 tests — BUILD SUCCESS
```

## `pgw-ingestor-pure-arrow` — Pure Arrow Ingestion Benchmark (`PureArrowIngestionBenchmarkTest`)

**Path:** XML → StAX streaming parse → `PureArrowBatchConsumer` → `ArrowStreamWriter` → `.arrow` files

Results from actual test run (2026-04-11, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔════════════════════════════════════════════════════════════════════════════════════════════╗
║   PURE ARROW INGESTION BENCHMARK SUMMARY (no DuckDB)                                       ║
╠══════════╦═══════════╦══════════════╦════════════════╦═══════════╦═════════╦══════════╣
║  Type    ║  XML (MB) ║  Parse (ms)  ║ Peak Off-Heap  ║  Tx Rows  ║ MB/sec  ║Arrow(MB) ║
╠══════════╬═══════════╬══════════════╬════════════════╬═══════════╬═════════╬══════════╣
║  Type A  ║    734.4  ║      12,799  ║      463.0 MB ║ 1,000,000 ║    57.4 ║   294.6 ║
║  Type B  ║    734.3  ║      11,172  ║      463.0 MB ║ 1,000,000 ║    65.7 ║   294.5 ║
║  Type C  ║  1,295.1  ║      18,219  ║      791.3 MB ║ 1,000,000 ║    71.1 ║   494.9 ║
║  Type D  ║      0.1  ║           6  ║        1.7 MB ║       200 ║    24.6 ║     0.1 ║
║  Type E  ║      0.1  ║           5  ║        1.7 MB ║       200 ║    29.5 ║     0.1 ║
║  Type F  ║  1,469.8  ║      20,363  ║      896.1 MB ║ 2,000,000 ║    72.2 ║   590.3 ║
║  Type G  ║  2,940.7  ║      41,519  ║    1,791.2 MB ║ 4,000,000 ║    70.8 ║ 1,181.7 ║
║  Type H  ║      1.5  ║          22  ║        2.0 MB ║     2,000 ║    66.7 ║     0.6 ║
║  Type I  ║      1.5  ║          22  ║        2.0 MB ║     2,000 ║    66.6 ║     0.6 ║
║  Type J  ║      0.0  ║           1  ║        1.7 MB ║         1 ║     1.7 ║     0.0 ║
╚══════════╩═══════════╩══════════════╩════════════════╩═══════════╩═════════╩══════════╝

  XML (MB)        = source XML file size on disk
  Parse (ms)      = StAX streaming parse + direct ArrowStreamWriter write (no DuckDB)
  Peak Off-Heap   = max Arrow allocator bytes while ALL ingested batches live in PureArrowInMemoryStore
                    IMPORTANT: grows O(total_rows) — all batches are kept in memory until store.close()
  Tx Rows         = transaction rows ingested
  MB/sec          = parse throughput (XML MB / parse seconds)
  Arrow (MB)      = combined size of the 3 exported .arrow files on disk
```

**Key findings — pure-Arrow pipeline:**
- **Peak off-heap is NOT bounded** — it grows proportionally with total rows:
  - Type A (1M tx): **463 MB** (≈ 16 batches × ~29 MB per batch)
  - Type F (2M tx): **896 MB** (≈ 31 batches × ~29 MB)
  - Type G (4M tx): **1,791 MB** (~1.75 GB — 4× allocator limit required)
- This is fundamentally different from the DuckDB path which stays at ~31 MB regardless of file size
- Parse speed is faster than DuckDB (see comparison below) because there is no SQL INSERT overhead
- **Type H** (10 × 200): 22 ms, 2.0 MB peak — multiple remittances, still tiny
- **Type I** (5 × 400): 22 ms, 2.0 MB peak — same total rows, different grouping
- **Type J** (1 × 1): 1 ms, 1.7 MB peak — **unitary baseline** (minimum viable payload)

## `pgw-ingestor-pure-arrow` — Memory Leak Verification

| Scenario | Iterations | Bytes Leaked |
|----------|----------:|------------:|
| Type D (valid, pure-Arrow) | 50 | **0** |
| Type E (invalid CtrlSum, pure-Arrow) | 50 | **0** |

✅ **Zero bytes leaked across all 100 iterations.**

---

# DuckDB vs Pure-Arrow Pipeline Comparison (`PipelineComparisonBenchmarkTest`)

This benchmark runs both pipelines back-to-back for each type and prints a side-by-side table.

**DuckDB pipeline total** = Parse+Insert ms + Export ms
**Pure-Arrow pipeline total** = Parse+Write ms (single step, no SQL engine)

Results from actual test run (2026-04-11, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║          PIPELINE COMPARISON — DuckDB  vs  Pure Arrow  (XML → .arrow files)                                                   ║
╠══════════╦══════════╦══════════════╦═══════════╦═══════════════╦══════════════╦═══════════════╦═══════════╦══════════════════╣
║  Type    ║ XML (MB) ║ DuckDB Prs+I ║ DuckDB Ex ║ DuckDB Peak   ║ PureArrow ms ║ PureArrow Pk  ║  Tx Rows  ║   Speedup (×)    ║
╠══════════╬══════════╬══════════════╬═══════════╬═══════════════╬══════════════╬═══════════════╬═══════════╬══════════════════╣
║  Type A  ║   734.4  ║       17,109 ║      633  ║       29.9 MB ║      11,734  ║      463.0 MB ║ 1,000,000 ║       1.51×     ║
║  Type B  ║   734.3  ║       12,886 ║      613  ║       29.9 MB ║       9,333  ║      463.0 MB ║ 1,000,000 ║       1.45×     ║
║  Type C  ║ 1,295.1  ║       56,962 ║    1,010  ║       49.7 MB ║      43,961  ║      791.3 MB ║ 1,000,000 ║       1.32×     ║
║  Type D  ║     0.1  ║           14 ║        5  ║        1.7 MB ║           7  ║        1.7 MB ║       200 ║       2.71×     ║
║  Type E  ║     0.1  ║           13 ║        5  ║        1.7 MB ║           8  ║        1.7 MB ║       200 ║       2.25×     ║
║  Type F  ║ 1,469.8  ║       58,787 ║    1,176  ║       29.9 MB ║      47,995  ║      896.1 MB ║ 2,000,000 ║       1.25×     ║
║  Type G  ║ 2,940.7  ║      118,085 ║    2,439  ║       29.9 MB ║      96,492  ║    1,791.2 MB ║ 4,000,000 ║       1.25×     ║
║  Type H  ║     1.5  ║           62 ║        7  ║        2.0 MB ║          51  ║        2.0 MB ║     2,000 ║       1.35×     ║
║  Type I  ║     1.5  ║           59 ║        7  ║        2.0 MB ║          52  ║        2.0 MB ║     2,000 ║       1.27×     ║
║  Type J  ║     0.0  ║            8 ║        5  ║        1.7 MB ║           2  ║        1.7 MB ║         1 ║       6.50×     ║
╚══════════╩══════════╩══════════════╩═══════════╩═══════════════╩══════════════╩═══════════════╩═══════════╩══════════════════╝

  DuckDB Prs+I   = StAX streaming parse + StreamingBatchConsumer INSERT into DuckDB (ms)
  DuckDB Ex      = ArrowIpc.export() for all 3 tables via C Data Interface (ms)
  DuckDB Peak    = peak Arrow off-heap during parse+insert (1 batch at a time → bounded ~30–50 MB)
  PureArrow ms   = StAX parse + PureArrowBatchConsumer + ArrowStreamWriter (ms)
  PureArrow Pk   = peak Arrow off-heap (ALL batches accumulated in store → O(total_rows))
  Speedup (×)    = DuckDB total (parse+insert+export) / PureArrow total (parse+write)
                   Values > 1.0× mean pure-Arrow is faster end-to-end
```

**Analysis:**

| Dimension | DuckDB Pipeline | Pure-Arrow Pipeline |
|-----------|----------------|---------------------|
| **Parse speed** (A/B, 1M rows) | ~17,742 ms total | ~11,734 ms (**1.5×** faster) |
| **Parse speed** (C, 1M×1 rows) | ~57,972 ms total | ~43,961 ms (**1.3×** faster) |
| **Parse speed** (F, 2M rows) | ~59,963 ms total | ~47,995 ms (**1.25×** faster) |
| **Parse speed** (G, 4M rows) | ~120,524 ms total | ~96,492 ms (**1.25×** faster) |
| **Peak off-heap** (A, 1M rows) | **~30 MB** (bounded — 1 batch) | **~463 MB** (all batches in store) |
| **Peak off-heap** (F, 2M rows) | **~30 MB** (bounded) | **~896 MB** |
| **Peak off-heap** (G, 4M rows) | **~30 MB** (bounded) | **~1,791 MB** |
| **Peak off-heap** (H/I, 2K rows) | **~2 MB** (bounded) | **~2 MB** (tiny — no accumulation effect) |
| **Peak off-heap** (J, 1 row) | **~1.7 MB** (bounded) | **~1.7 MB** (unitary baseline) |
| **Arrow file output** | Same size (same schema) | Same size |
| **SQL queries at ingest** | ✅ Available immediately | ❌ Not available |
| **Pod memory budget** | Low: ~31 MB Arrow + DuckDB RSS | High: grows O(total_rows) |

**Key trade-offs:**

⚡ **Pure-Arrow is faster at ingest** because there is no DuckDB INSERT overhead (SQL engine
receives, processes, and indexes each batch). This advantage is ~1.4–2× for typical types.

⚠️ **Pure-Arrow uses far more off-heap memory** because all `ArrowRecordBatch` objects are
kept in `PureArrowInMemoryStore` until `store.close()`. For 4M rows the store holds
~1.75 GB of Arrow off-heap. The DuckDB pipeline keeps only 1 batch (~30 MB) in Arrow memory
at any time.

**When to choose each pipeline:**
- **DuckDB pipeline** → when downstream SQL analytics on the ingested data are needed, or when
  memory is constrained (pods with < 512 MB headroom over DuckDB RSS)
- **Pure-Arrow pipeline** → when only `.arrow` IPC files are needed (e.g. write and hand off),
  memory is ample (>= 500 MB per 1M rows), and maximum ingest throughput matters

---
