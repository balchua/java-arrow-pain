# PGW — Test Results & Benchmark Report

Multi-module build: `pgw-common` + `pgw-domain` + `pgw-ingestor` + `pgw-ingestor-pure-arrow` + `pgw-validator` + `pgw-validator-pure-arrow`

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

Results from actual test run (2026-04-10, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║       Ingestion Benchmark — XML → StAX Parse → DuckDB INSERT → ArrowIpc.export()                       ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  Type    ║ XML (MB) ║ Parse+Ins ms ║ Export ms  ║ Peak Off-Heap  ║ Arrow (MB) ║  Tx Rows  ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  Type A  ║   734.4  ║      15,758  ║       987  ║   31,309,824   ║    294.64 ║ 1,000,000 ║
║  Type B  ║   734.3  ║      14,317  ║       788  ║   31,309,824   ║    294.54 ║ 1,000,000 ║
║  Type C  ║ 1,295.1  ║      31,058  ║     1,110  ║   52,101,120   ║    494.90 ║ 1,000,000 ║
║  Type D  ║     0.1  ║          16  ║         5  ║    1,785,856   ║      0.06 ║       200 ║
║  Type E  ║     0.1  ║          12  ║         5  ║    1,785,856   ║      0.06 ║       200 ║
║  Type F  ║ 1,469.8  ║      29,716  ║     1,799  ║   31,309,824   ║    590.34 ║ 2,000,000 ║
║  Type G  ║ 2,940.7  ║      61,294  ║     4,498  ║   31,309,824   ║  1,181.73 ║ 4,000,000 ║
║  Type H  ║     1.5  ║          30  ║         7  ║    2,080,768   ║      0.59 ║     2,000 ║
║  Type I  ║     1.5  ║          29  ║         6  ║    2,080,768   ║      0.59 ║     2,000 ║
║  Type J  ║     0.0  ║           9  ║         4  ║    1,785,856   ║      0.01 ║         1 ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  TOTAL   ║ 7,177.5  ║     152,239  ║     9,209  ║   52,101,120   ║  2,857.46 ║ 9,004,401 ║
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
- Type G (4M rows) scales linearly with Type F: 61,294 ms vs 29,716 ms
- DuckDB export is fast (< 4.5 s even for 4M rows) since it reads from DuckDB, not XML
- **Type H** (10 × 200 = 2,000 tx): 30 ms parse, 7 ms export — multi-remittance correctness baseline
- **Type I** (5 × 400 = 2,000 tx): 29 ms parse, 6 ms export — same total rows as H, different grouping
- **Type J** (1 × 1 = 1 tx): 9 ms parse, 4 ms export — **unitary baseline**, minimal XML payload

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

Results from actual test run (2026-04-10, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔════════════════════════════════════════════════════════════════════════════════════════════╗
║   PURE ARROW INGESTION BENCHMARK SUMMARY (no DuckDB)                                       ║
╠══════════╦═══════════╦══════════════╦════════════════╦═══════════╦═════════╦══════════╣
║  Type    ║  XML (MB) ║  Parse (ms)  ║ Peak Off-Heap  ║  Tx Rows  ║ MB/sec  ║Arrow(MB) ║
╠══════════╬═══════════╬══════════════╬════════════════╬═══════════╬═════════╬══════════╣
║  Type A  ║    734.4  ║      12,289  ║      463.0 MB  ║ 1,000,000 ║    59.8 ║   294.6  ║
║  Type B  ║    734.3  ║      11,115  ║      463.0 MB  ║ 1,000,000 ║    66.1 ║   294.5  ║
║  Type C  ║  1,295.1  ║      16,390  ║      791.3 MB  ║ 1,000,000 ║    79.0 ║   494.9  ║
║  Type D  ║      0.1  ║           6  ║        1.7 MB  ║       200 ║    24.6 ║     0.1  ║
║  Type E  ║      0.1  ║           6  ║        1.7 MB  ║       200 ║    24.6 ║     0.1  ║
║  Type F  ║  1,469.8  ║      17,586  ║      896.1 MB  ║ 2,000,000 ║    83.6 ║   590.3  ║
║  Type G  ║  2,940.7  ║      35,273  ║    1,791.2 MB  ║ 4,000,000 ║    83.4 ║ 1,181.7  ║
║  Type H  ║      1.5  ║          19  ║        2.0 MB  ║     2,000 ║    77.3 ║     0.6  ║
║  Type I  ║      1.5  ║          20  ║        2.0 MB  ║     2,000 ║    73.3 ║     0.6  ║
║  Type J  ║      0.0  ║           2  ║        1.7 MB  ║         1 ║     0.8 ║     0.0  ║
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
- **Type H** (10 × 200): 19 ms, 2.0 MB peak — multiple remittances, still tiny
- **Type I** (5 × 400): 20 ms, 2.0 MB peak — same total rows, different grouping
- **Type J** (1 × 1): 2 ms, 1.7 MB peak — **unitary baseline** (minimum viable payload)

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

Results from actual test run (2026-04-10, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║          PIPELINE COMPARISON — DuckDB  vs  Pure Arrow  (XML → .arrow files)                                                   ║
╠══════════╦══════════╦══════════════╦═══════════╦═══════════════╦══════════════╦═══════════════╦═══════════╦══════════════════╣
║  Type    ║ XML (MB) ║ DuckDB Prs+I ║ DuckDB Ex ║ DuckDB Peak   ║ PureArrow ms ║ PureArrow Pk  ║  Tx Rows  ║   Speedup (×)    ║
╠══════════╬══════════╬══════════════╬═══════════╬═══════════════╬══════════════╬═══════════════╬═══════════╬══════════════════╣
║  Type A  ║   734.4  ║       15,758 ║       987 ║      29.9 MB  ║      12,289  ║     463.0 MB  ║ 1,000,000 ║        1.35×     ║
║  Type B  ║   734.3  ║       14,317 ║       788 ║      29.9 MB  ║      11,115  ║     463.0 MB  ║ 1,000,000 ║        1.36×     ║
║  Type C  ║ 1,295.1  ║       31,058 ║     1,110 ║      49.7 MB  ║      16,390  ║     791.3 MB  ║ 1,000,000 ║        1.96×     ║
║  Type D  ║     0.1  ║           16 ║         5 ║       1.7 MB  ║           6  ║       1.7 MB  ║       200 ║        3.50×     ║
║  Type E  ║     0.1  ║           12 ║         5 ║       1.7 MB  ║           6  ║       1.7 MB  ║       200 ║        2.83×     ║
║  Type F  ║ 1,469.8  ║       29,716 ║     1,799 ║      29.9 MB  ║      17,586  ║     896.1 MB  ║ 2,000,000 ║        1.79×     ║
║  Type G  ║ 2,940.7  ║       61,294 ║     4,498 ║      29.9 MB  ║      35,273  ║   1,791.2 MB  ║ 4,000,000 ║        1.86×     ║
║  Type H  ║     1.5  ║           30 ║         7 ║       2.0 MB  ║          19  ║       2.0 MB  ║     2,000 ║        1.95×     ║
║  Type I  ║     1.5  ║           29 ║         6 ║       2.0 MB  ║          20  ║       2.0 MB  ║     2,000 ║        1.75×     ║
║  Type J  ║     0.0  ║            9 ║         4 ║       1.7 MB  ║           2  ║       1.7 MB  ║         1 ║        6.50×     ║
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
| **Parse speed** (A/B, 1M rows) | ~16,000 ms total | ~11,700 ms (**1.4×** faster) |
| **Parse speed** (C, 1M×1 rows) | ~32,168 ms total | ~16,390 ms (**2×** faster) |
| **Parse speed** (F, 2M rows) | ~31,515 ms total | ~17,586 ms (**1.8×** faster) |
| **Parse speed** (G, 4M rows) | ~65,792 ms total | ~35,273 ms (**1.9×** faster) |
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

# `pgw-validator` — Arrow → DuckDB Load + SQL Validation Tests

**Responsibility:** Load pre-exported `.arrow` files into DuckDB via `ArrowIpc.load()`,
then run the chainable SQL validation pipeline. Owns all domain (model, VOs, DAL, validators).

## How to Run `pgw-validator` Tests

```bash
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator

# Domain validation correctness only (fast)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-validator -Dtest=ValidationTest
```

## `pgw-validator` Test Classes

| Test Class | What It Tests | Tests |
|-----------|--------------|------:|
| `ValidationTest` | Domain validation correctness: Type D passes, Type E fails with 3 errors | 2 |
| `ArrowFileLoadBenchmarkTest` | Arrow file → DuckDB load time only (no validation), all 7 types | 1 |
| `ValidationBenchmarkTest` | Arrow → DuckDB load time **+** SQL validation time, separated, all 7 types | 1 |
| **Total** | | **4** |

## `pgw-validator` Test Results

```
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0  -- ValidationTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ArrowFileLoadBenchmarkTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ValidationBenchmarkTest

pgw-validator :   4 tests — BUILD SUCCESS
```

---

# `pgw-validator-pure-arrow` — Arrow-backed Validation (no DuckDB)

**Responsibility:** Load pre-exported `.arrow` IPC files via `ArrowPaymentRepositoryLoader`
into an in-memory pure-Arrow repository, then run the same chainable `ValidationPipeline`.
No DuckDB, no JDBC, no SQL — all validation logic is implemented in pure Java scanning
Arrow vectors directly.

**Pipeline:**
```
pain.001 XML on disk
     ↓  [PureArrowIngestor — XML→Arrow, no DuckDB]
.arrow files (3 × Arrow IPC stream files)
     ↓  [ArrowPaymentRepositoryLoader — ArrowStreamReader + VectorLoader]
ArrowPaymentRepositoryImpl (in-memory Arrow vectors)
     ↓  [ValidationPipeline.standard()]
ValidationContext (errors / warnings)
```

**Modular DAL design:**
- `ArrowTableLoader` — utility that reads Arrow IPC stream files and materialises record batches
- `ArrowMessageTable` — typed column accessor for the `message` table (one row per GrpHdr)
- `ArrowRemittanceTable` — typed column accessor for the `remittance` table (one row per PmtInf)
- `ArrowTransactionTable` — typed column accessor for the `transactions` table (one row per CdtTrfTxInf)
- `ArrowPaymentRepositoryImpl` — implements `PaymentRepository` by composing the three table classes
- `ArrowPaymentRepositoryLoader` — factory: loads files → builds the repository

## How to Run `pgw-validator-pure-arrow` Tests

```bash
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator-pure-arrow

# Correctness tests only (fast)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-validator-pure-arrow -Dtest=ArrowValidationTest

# Benchmark only (all types A–J)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator-pure-arrow -Dtest=ArrowValidationBenchmarkTest

# Comparison benchmark (DuckDB SQL vs pure-Arrow Java, all types A–J)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator-pure-arrow -Dtest=ValidatorComparisonBenchmarkTest
```

## `pgw-validator-pure-arrow` Test Classes

| Test Class | What It Tests | Tests |
|-----------|--------------|------:|
| `ArrowValidationTest` | Correctness: Types D (passes), E (CtrlSum errors), H, J | 4 |
| `ArrowValidationBenchmarkTest` | Arrow ingest + Arrow load + pure-Java validation, all types A–J | 1 |
| `ValidatorComparisonBenchmarkTest` | Side-by-side: DuckDB SQL validation vs pure-Arrow Java validation, A–J | 1 |
| **Total** | | **6** |

## `pgw-validator-pure-arrow` Test Results

```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0  -- ArrowValidationTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ArrowValidationBenchmarkTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ValidatorComparisonBenchmarkTest

pgw-validator-pure-arrow :  6 tests — BUILD SUCCESS
```

## `pgw-validator-pure-arrow` — Benchmark (Types A–J)

Results from actual test run (2026-04-10, Java 25 Temurin 25.0.2, `-Xmx4g`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║    Pure-Arrow Validation Benchmark — XML→Arrow + Arrow Load + SQL-equivalent Validation (no DuckDB)                                  ║
╠══════════╦════════════╦═══════════════╦═══════════╦══════════════╦════════════════╦═══════════╦══════════════╦═══════════╣
║  Type    ║ Arrow (KB) ║  Ingest ms    ║  Load ms  ║ Validate ms  ║ Peak Off-Heap  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬═══════════════╬═══════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  Type A   ║    301,712 ║        12,263 ║        58 ║        3,110 ║    797,554,824 ║ 1,000,000 ║          322 ║ ✓ PASSED  ║
║  Type B   ║    301,604 ║        10,697 ║        50 ║        3,045 ║    797,444,240 ║ 1,000,000 ║          328 ║ ✓ PASSED  ║
║  Type C   ║    506,780 ║        17,433 ║        77 ║        6,237 ║  1,397,565,328 ║ 1,000,000 ║          160 ║ ✓ PASSED  ║
║  Type D   ║         65 ║             6 ║         2 ║            1 ║      1,852,672 ║       200 ║          200 ║ ✓ PASSED  ║
║  Type E   ║         65 ║             4 ║         1 ║            4 ║      1,852,672 ║       200 ║           50 ║ ✗ 3 err   ║
║  Type F   ║    604,504 ║        19,096 ║       101 ║        6,125 ║  1,564,876,240 ║ 2,000,000 ║          327 ║ ✓ PASSED  ║
║  Type G   ║  1,210,090 ║        37,998 ║       300 ║       12,137 ║  3,117,643,784 ║ 4,000,000 ║          330 ║ ✓ PASSED  ║
║  Type H   ║        604 ║            22 ║         1 ║            6 ║      3,133,696 ║     2,000 ║          333 ║ ✓ PASSED  ║
║  Type I   ║        603 ║            21 ║         1 ║            6 ║      3,131,648 ║     2,000 ║          333 ║ ✓ PASSED  ║
║  Type J   ║          6 ║             1 ║         1 ║            0 ║      1,787,648 ║         1 ║            1 ║ ✓ PASSED  ║
╠══════════╬════════════╬═══════════════╬═══════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL    ║  2,926,033 ║        97,541 ║       592 ║       30,671 ║  3,117,643,784 ║ 9,004,401 ║          294 ║ —         ║
╚══════════╩════════════╩═══════════════╩═══════════╩══════════════╩════════════════╩═══════════╩══════════════╩═══════════╝

  Arrow (KB)     = combined size of the 3 .arrow IPC files on disk
  Ingest ms      = XML → StAX parse → PureArrowBatchConsumer → .arrow files (no DuckDB)
  Load ms        = time to materialise .arrow files into ArrowPaymentRepositoryImpl
  Validate ms    = time to run ValidationPipeline.standard() in pure Java/Arrow (no SQL)
  Peak Off-Heap  = max Arrow allocator off-heap bytes (includes ingested + loaded batches)
                   NOTE: much larger than DuckDB path because all batches stay in Arrow allocator
  rows/ms (val)  = transaction row scan throughput during validation
```

## `pgw-validator-pure-arrow` — Streaming Iteration (StreamingTransactionIteratorValidator)

```
╔══════════════════════════════════════════════════════════════════════╗
║   Streaming Iteration — StreamingTransactionIteratorValidator         ║
╠══════════╦═══════════╦════════════════╦══════════════╣
║  Type    ║  Tx Rows  ║ Streaming ms   ║ rows/ms (str)║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  Type A   ║ 1,000,000 ║          1,506 ║          664 ║
║  Type B   ║ 1,000,000 ║          1,515 ║          660 ║
║  Type C   ║ 1,000,000 ║          1,559 ║          641 ║
║  Type D   ║       200 ║              0 ║          200 ║
║  Type E   ║       200 ║              0 ║          200 ║
║  Type F   ║ 2,000,000 ║          3,029 ║          660 ║
║  Type G   ║ 4,000,000 ║          5,999 ║          667 ║
║  Type H   ║     2,000 ║              2 ║        1,000 ║
║  Type I   ║     2,000 ║              2 ║        1,000 ║
║  Type J   ║         1 ║              0 ║            1 ║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  TOTAL    ║ 9,004,401 ║         13,612 ║          662 ║
╚══════════╩═══════════╩════════════════╩══════════════╝

  Streaming ms   = time for StreamingTransactionIteratorValidator to iterate all rows
                   directly from Arrow vectors (no SQL, no JDBC cursor)
  ► Grand Total: 13,612 ms for 9,004,401 rows  ≈ 662 rows/ms pure Arrow vector iteration
```

## `pgw-validator-pure-arrow` — DuckDB SQL vs Pure-Arrow Comparison (ValidatorComparisonBenchmarkTest)

Results from actual test run (2026-04-10, Java 25 Temurin 25.0.2, `-Xmx4g`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║    Validator Pipeline Comparison — DuckDB SQL vs Pure-Arrow Java (same ValidationPipeline.standard())                                                    ║
╠══════════╦══════════════════════════════════════════════════════════════════╦══════════════════════════════════════════════════════════════════════╦════════╣
║          ║              DuckDB path (SQL)                                   ║            Pure-Arrow path (Java)                                     ║        ║
║  Type    ║ Ingest ms │ Load ms  │ Val ms   │ Peak Off-Heap  │  Result       ║ Ingest ms │ Load ms  │ Val ms   │ Peak Off-Heap  │  Result       ║Speedup ║
╠══════════╬══════════════════════════════════════════════════════════════════╬══════════════════════════════════════════════════════════════════════╬════════╣
║  Type A   ║    17,579 │      728 │       83 │    121,587,294 │ ✓ PASS        ║    11,689 │       72 │    3,260 │    797,554,824 │ ✓ PASS        ║  1.22x ║
║  Type B   ║    13,523 │      630 │       84 │    121,541,462 │ ✓ PASS        ║     9,355 │       66 │    3,178 │    797,444,240 │ ✓ PASS        ║  1.13x ║
║  Type C   ║    59,281 │    1,312 │      330 │    121,511,222 │ ✓ PASS        ║    44,551 │       96 │    6,464 │  1,397,565,328 │ ✓ PASS        ║  1.19x ║
║  Type D   ║        90 │       16 │        8 │      1,787,592 │ ✓ PASS        ║         8 │        1 │        2 │      1,852,672 │ ✓ PASS        ║ 10.36x ║
║  Type E   ║        29 │       19 │       12 │      1,787,592 │ ✗ 3 err       ║         8 │        1 │        2 │      1,852,672 │ ✗ 3 err       ║  5.45x ║
║  Type F   ║    61,188 │    1,296 │      134 │    121,904,534 │ ✓ PASS        ║    48,578 │      136 │    6,414 │  1,564,876,240 │ ✓ PASS        ║  1.14x ║
║  Type G   ║   122,073 │    1,982 │      242 │    121,970,078 │ ✓ PASS        ║    96,639 │      388 │   12,631 │  3,117,643,784 │ ✓ PASS        ║  1.13x ║
║  Type H   ║       146 │       19 │        9 │      2,113,536 │ ✓ PASS        ║        53 │        1 │        7 │      3,133,696 │ ✓ PASS        ║  2.85x ║
║  Type I   ║        80 │       17 │        7 │      2,113,536 │ ✓ PASS        ║        53 │        0 │        6 │      3,131,648 │ ✓ PASS        ║  1.76x ║
║  Type J   ║        24 │       15 │        7 │      1,787,592 │ ✓ PASS        ║         2 │        0 │        0 │      1,787,648 │ ✓ PASS        ║ 23.00x ║
╚══════════╩══════════════════════════════════════════════════════════════════╩══════════════════════════════════════════════════════════════════════╩════════╝

  ► Total DuckDB  path: 280,963 ms for 9,004,401 transaction rows
  ► Total Arrow   path: 243,661 ms for 9,004,401 transaction rows
  ► Overall speedup: 1.15x (Arrow vs DuckDB end-to-end)
```

**Key observations:**
- **Same `ValidationPipeline.standard()`** runs unchanged on both backends (polymorphism through `PaymentRepository` interface).
- **Validation results are identical**: both pipelines agree on every pass/fail decision (Type E: 3 CtrlSum errors each).
- **Ingest is faster** with pure Arrow (no DuckDB INSERT round-trip): 1.1–6.5× faster.
- **Validation is slower** in pure Arrow (iterating Arrow vectors in Java vs DuckDB's compiled SQL): ~15–50× slower for large types.
- **Load is faster** in pure Arrow: memory copy via `VectorLoader` is faster than Arrow C Data Interface round-trip through DuckDB.
- **Peak off-heap is much larger** in pure Arrow: Arrow allocator holds ALL batches in memory simultaneously (~760 MB for 1M tx), whereas DuckDB holds data in its own managed memory and the Arrow allocator stays bounded at ~120 MB for the C Data Interface transfer.
- **End-to-end speed**: pure Arrow is ~1.15× faster overall (ingest savings outweigh validation overhead for files dominated by ingest time).

**When to choose each validator backend:**
- **DuckDB validator** (`pgw-validator`) → when memory is constrained (needs only ~120 MB Arrow off-heap vs ~760 MB+), or when OLAP-scale SQL validation logic is complex (window functions, joins across tables, regex).
- **Pure-Arrow validator** (`pgw-validator-pure-arrow`) → when DuckDB is not available, when the Arrow files are already in memory, or when validation logic is simple per-row checks that translate easily to Java predicates.

---

# Full Test Run Summary

```
══════════════════════════════════════════════════════════════
  pgw-common
══════════════════════════════════════════════════════════════
  TestFileGenerator          :  2 tests — PASS
  ─────────────────────────────────────────────
  Subtotal                   :  2 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  pgw-domain
══════════════════════════════════════════════════════════════
  (no tests — pure domain model module)
  ─────────────────────────────────────────────
  Subtotal                   :  0 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  pgw-ingestor
══════════════════════════════════════════════════════════════
  ParsePipelineTest          :  4 tests — PASS
  StreamingPipelineTest      :  4 tests — PASS
  MemoryLeakVerificationTest :  3 tests — PASS  (103 iterations, 0 bytes leaked)
  IngestionBenchmarkTest     :  1 test  — PASS  (Types A–J, peak ~31–52 MB DuckDB off-heap)
  ─────────────────────────────────────────────
  Subtotal                   : 12 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  pgw-ingestor-pure-arrow
══════════════════════════════════════════════════════════════
  PureArrowParsePipelineTest        :  5 tests — PASS
  PureArrowStreamingPipelineTest    :  3 tests — PASS
  PureArrowMemoryLeakVerificationTest: 2 tests — PASS  (100 iterations, 0 bytes leaked)
  PureArrowIngestionBenchmarkTest   :  1 test  — PASS  (Types A–J, peak 463 MB–1.8 GB off-heap)
  PipelineComparisonBenchmarkTest   :  1 test  — PASS  (DuckDB vs Pure Arrow A–J, 1.4–6.5× speedup)
  ─────────────────────────────────────────────
  Subtotal                          : 12 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  pgw-validator
══════════════════════════════════════════════════════════════
  ValidationTest             :  2 tests — PASS  (Type D passes, Type E fails correctly)
  ArrowFileLoadBenchmarkTest :  1 test  — PASS
  ValidationBenchmarkTest    :  1 test  — PASS
  ─────────────────────────────────────────────
  Subtotal                   :  4 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  pgw-validator-pure-arrow
══════════════════════════════════════════════════════════════
  ArrowValidationTest              :  4 tests — PASS  (D passes, E fails, H & J pass)
  ArrowValidationBenchmarkTest     :  1 test  — PASS  (Types A–J, peak 1.8 MB–3.1 GB off-heap)
  ValidatorComparisonBenchmarkTest :  1 test  — PASS  (DuckDB SQL vs Arrow Java, 1.1–23× speedup)
  ─────────────────────────────────────────────
  Subtotal                         :  6 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  TOTAL                      : 36 tests — BUILD SUCCESS
══════════════════════════════════════════════════════════════
```

**All 36 tests pass. Benchmark data above is from actual test runs on 2026-04-10 (Java 25 Temurin 25.0.2, -Xmx4g).**
