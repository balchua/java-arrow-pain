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
║  Type A  ║   734.4  ║      15,826  ║       585  ║   31,309,824   ║    294.64 ║ 1,000,000 ║
║  Type B  ║   734.3  ║      14,498  ║       544  ║   31,309,824   ║    294.54 ║ 1,000,000 ║
║  Type C  ║ 1,295.1  ║      30,942  ║       901  ║   52,101,120   ║    494.90 ║ 1,000,000 ║
║  Type D  ║     0.1  ║          19  ║         5  ║    1,785,856   ║      0.06 ║       200 ║
║  Type E  ║     0.1  ║          13  ║         5  ║    1,785,856   ║      0.06 ║       200 ║
║  Type F  ║ 1,469.8  ║      30,657  ║     1,069  ║   31,309,824   ║    590.34 ║ 2,000,000 ║
║  Type G  ║ 2,940.7  ║      63,367  ║     2,243  ║   31,309,824   ║  1,181.73 ║ 4,000,000 ║
║  Type H  ║     1.5  ║          31  ║         8  ║    2,080,768   ║      0.59 ║     2,000 ║
║  Type I  ║     1.5  ║          29  ║         6  ║    2,080,768   ║      0.59 ║     2,000 ║
║  Type J  ║     0.0  ║           7  ║         4  ║    1,785,856   ║      0.01 ║         1 ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  TOTAL   ║ 7,177.5  ║     155,389  ║     5,370  ║   52,101,120   ║  2,857.46 ║ 9,004,401 ║
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

**Responsibility:** StAX XML parsing → `PureArrowBatchConsumer` (TransferPair per batch) →
`PureArrowInMemoryStore` → `ArrowStreamWriter` → `.arrow` files. No DuckDB during ingest.

**⚠ Memory model difference:** Unlike the DuckDB pipeline, the pure-Arrow pipeline **accumulates
ALL `ArrowRecordBatch` objects** in `PureArrowInMemoryStore` until `store.close()` is called.
This means peak off-heap grows proportionally to the total number of rows ingested, not just 1 batch.

## How to Run `pgw-ingestor-pure-arrow` Tests

```bash
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor-pure-arrow

# Ingestion benchmark only
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor-pure-arrow -Dtest=PureArrowIngestionBenchmarkTest
```

## `pgw-ingestor-pure-arrow` Test Classes

| Test Class | What It Tests | Tests |
|-----------|--------------|------:|
| `PureArrowParsePipelineTest` | Parse correctness: row counts, IPC file creation, ArrowStreamReader round-trip | 5 |
| `PureArrowStreamingPipelineTest` | Memory footprint, row counts, multi-ingest allocator sharing | 3 |
| `PureArrowMemoryLeakVerificationTest` | 50-iteration zero-leak test for Types D + E | 2 |
| `PureArrowIngestionBenchmarkTest` | XML → Arrow IPC benchmark for Types A–J (no DuckDB) | 1 |
| **Total** | | **11** |

## `pgw-ingestor-pure-arrow` Test Results

```
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0  -- PureArrowParsePipelineTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0  -- PureArrowStreamingPipelineTest
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0  -- PureArrowMemoryLeakVerificationTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- PureArrowIngestionBenchmarkTest

pgw-ingestor-pure-arrow  :  11 tests — BUILD SUCCESS
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
║  Type A  ║    734.4  ║      12,783  ║      463.0 MB ║ 1,000,000 ║    57.4 ║   294.6 ║
║  Type B  ║    734.3  ║      10,855  ║      463.0 MB ║ 1,000,000 ║    67.6 ║   294.5 ║
║  Type C  ║  1,295.1  ║      18,447  ║      791.3 MB ║ 1,000,000 ║    70.2 ║   494.9 ║
║  Type D  ║      0.1  ║           6  ║        1.7 MB ║       200 ║    24.6 ║     0.1 ║
║  Type E  ║      0.1  ║           5  ║        1.7 MB ║       200 ║    29.5 ║     0.1 ║
║  Type F  ║  1,469.8  ║      19,321  ║      896.1 MB ║ 2,000,000 ║    76.1 ║   590.3 ║
║  Type G  ║  2,940.7  ║      38,456  ║    1,791.2 MB ║ 4,000,000 ║    76.5 ║ 1,181.7 ║
║  Type H  ║      1.5  ║          22  ║        2.0 MB ║     2,000 ║    66.7 ║     0.6 ║
║  Type I  ║      1.5  ║          21  ║        2.0 MB ║     2,000 ║    69.8 ║     0.6 ║
║  Type J  ║      0.0  ║           2  ║        1.7 MB ║         1 ║     0.8 ║     0.0 ║
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
