# PGW — Test Results & Benchmark Report

Multi-module build (`pgw-ingestor` + `pgw-validator`), package root `com.pgw`.

**Environment:** Java 25 (Temurin 25.0.2), Maven 3.9, `-Xmx4g`,
`--add-opens=java.base/java.nio=ALL-UNNAMED`

**No extensions required.** Arrow export and import use DuckDB's built-in C Data Interface
(`DuckDBResultSet.arrowExportStream` / `registerArrowStream`) — works in air-gapped environments.

---

## Pipeline Architecture

```
XML → StAX parse → StreamingBatchConsumer → DuckDB (single sink)
                                                  ↓
                                    ArrowIpc.export()
                                    DuckDBResultSet.arrowExportStream(allocator, 65536)
                                    [C Data Interface — no extension]
                                                  ↓
                                         *.arrow files on disk
                                                  ↓  [ArrowIpc.load()]
                                    ArrowStreamReader → registerArrowStream
                                    CREATE TABLE AS SELECT * FROM stream
                                                  ↓
                                    Validator in-process DuckDB
                                                  ↓
                                    SQL validation pipeline
```

---

## How to Run All Tests

```bash
# No extension installation needed — ArrowIpc uses the C Data Interface built into DuckDB JDBC.

# Full suite — both modules (all 16 tests, ~10 min on first run for Types F & G)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor,pgw-validator

# Standardized run with timestamped log saved to test-results/
./run_validation_tests.sh
```

---

---

# `pgw-ingestor` — XML Parse + Arrow Export Tests

**Responsibility:** StAX XML parsing → Arrow IPC batches → DuckDB live INSERT →
`ArrowIpc.export()` → `.arrow` files on disk. No domain knowledge.

## How to Run `pgw-ingestor` Tests

```bash
# All ingestor tests (fast tests < 10 s; ingestion benchmark ~1.5 min on first run)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor

# Individual test classes
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor -Dtest=ParsePipelineTest

MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor -Dtest=StreamingPipelineTest

MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor -Dtest=MemoryLeakVerificationTest

# Ingestion benchmark only (generates Types A–G on first run; F & G take ~5 min)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor -Dtest=IngestionBenchmarkTest
```

## `pgw-ingestor` Test Classes

| Test Class | What It Tests | Tests |
|-----------|--------------|------:|
| `ParsePipelineTest` | StAX parser correctness: row counts, field values, edge cases | 4 |
| `StreamingPipelineTest` | Memory footprint; DuckDB row counts; `ArrowIpc.export` + `ArrowIpc.load` round-trip | 4 |
| `MemoryLeakVerificationTest` | 50-iteration streaming parse stress test — zero bytes leaked | 3 |
| `IngestionBenchmarkTest` | XML → DuckDB INSERT → Arrow export benchmark for Types A–G (parse ms, export ms, peak memory) | 1 |
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

**Path:** XML → StAX streaming parse → `StreamingBatchConsumer` (INSERT per batch) → DuckDB →
`ArrowIpc.export()` (C Data Interface, no extension) → `.arrow` files

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║       Ingestion Benchmark — XML → StAX Parse → DuckDB INSERT → ArrowIpc.export()                       ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  Type    ║ XML (MB) ║ Parse+Ins ms ║ Export ms  ║ Peak Off-Heap  ║ Arrow (MB) ║  Tx Rows  ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  Type A  ║   734.4  ║      15,843  ║       578  ║   31,309,824   ║    294.64 ║ 1,000,000 ║
║  Type B  ║   734.3  ║      14,186  ║       560  ║   31,309,824   ║    294.54 ║ 1,000,000 ║
║  Type C  ║ 1,295.1  ║      30,048  ║       910  ║   52,101,120   ║    494.90 ║ 1,000,000 ║
║  Type D  ║     0.1  ║          13  ║         5  ║    1,785,856   ║      0.06 ║       200 ║
║  Type E  ║     0.1  ║          13  ║         6  ║    1,785,856   ║      0.06 ║       200 ║
║  Type F  ║ 1,469.8  ║      29,590  ║     1,087  ║   31,309,824   ║    590.34 ║ 2,000,000 ║
║  Type G  ║ 2,940.7  ║      59,827  ║     2,340  ║   31,309,824   ║  1,181.73 ║ 4,000,000 ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  TOTAL   ║ 7,174.6  ║     149,520  ║     5,486  ║   52,101,120   ║  2,856.27 ║ 9,000,400 ║
╚══════════╩══════════╩══════════════╩════════════╩════════════════╩════════════╩═══════════╝

  XML (MB)       = source XML file size on disk
  Parse+Ins ms   = StAX streaming parse + StreamingBatchConsumer INSERT into DuckDB
  Export ms      = ArrowIpc.export() for all 3 tables (C Data Interface, no extension)
  Peak Off-Heap  = peak Arrow allocator off-heap bytes during parse+insert phase
  Arrow (MB)     = combined size of the 3 exported .arrow files on disk
  Tx Rows        = transaction rows ingested
```

**Key findings:**
- Type C (1M PmtInf × 1 TxInf) is the slowest to ingest (30,048 ms) due to 1M separate `INSERT INTO … SELECT *` batch calls — one per remittance group
- Types A and B are ~2× faster because fewer, larger batches (1–2 `PmtInf` groups → ~16 batches of 65k rows each)
- **Type F (1 PmtInf × 2M TxInf)** ingests 2M transactions in 29,590 ms — similar throughput to Type A (1M in 15,843 ms), confirming near-linear scaling
- **Type G (1 PmtInf × 4M TxInf)** ingests 4M transactions in 59,827 ms — scales linearly with row count
- Arrow export is fast in all cases (578–2,340 ms) — bounded to one 65k-row batch at a time off-heap
- **Peak off-heap stays bounded at ~31 MB** for all single-remittance types (A, B, D, E, F, G) — even with 4M transactions, the streaming architecture keeps memory flat

## `pgw-ingestor` — Memory Leak Verification (`MemoryLeakVerificationTest`)

Runs Types D and E 50× each through the streaming parse → DuckDB pipeline,
checking that `allocator.getAllocatedMemory() == 0` after every iteration.

| Scenario | Iterations | Bytes Leaked |
|----------|----------:|------------:|
| Type D (valid, streaming) | 50 | **0** |
| Type E (invalid CtrlSum, streaming) | 50 | **0** |
| Type D (short) | 3 | **0** |

✅ **Zero bytes leaked across all 103 iterations.**

## `pgw-ingestor` — `StreamingPipelineTest` Details

| Test | What It Verifies |
|------|-----------------|
| `testMemoryFootprintIsFlat` | Peak off-heap ≤ 6 MB for Type D (2×100 rows) — confirms streaming, not accumulation |
| `testDuckDbRowCountsMatchParsed` | Row counts in DuckDB match `ParseStats` returned by parser |
| `testArrowFilesExported` | `ArrowIpc.export` writes non-empty files; `ArrowIpc.load` round-trips with correct row counts |
| `testOutputPathParameter` | Files are written to the specified custom output directory |

---

---

# `pgw-validator` — Arrow → DuckDB Load + SQL Validation Tests

**Responsibility:** Load pre-exported `.arrow` files into DuckDB via `ArrowIpc.load()`,
then run the chainable SQL validation pipeline. Owns all domain (model, VOs, DAL, validators).

## How to Run `pgw-validator` Tests

```bash
# All validator tests (first run ~2 min — generates large Arrow files for Types A–C)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator --also-make

# Domain validation correctness only (fast, no file generation)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-validator --also-make -Dtest=ValidationTest

# Arrow → DuckDB load benchmark only
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator --also-make -Dtest=ArrowFileLoadBenchmarkTest

# Validation-stage benchmark (DuckDB load + SQL validation, separated timings)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator --also-make -Dtest=ValidationBenchmarkTest

# Run the application (no benchmark, no tests)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn exec:java -pl pgw-validator -Dexec.args="path/to/pain001.xml"
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

pgw-validator :   4 tests — BUILD SUCCESS  (first run ~10 min — generates large files for Types F & G)
```

## `pgw-validator` — Arrow File → DuckDB Load Benchmark (`ArrowFileLoadBenchmarkTest`)

**Path:** pre-exported `.arrow` files → `ArrowIpc.load()` → DuckDB `CREATE TABLE AS SELECT`

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║           Arrow File -> DuckDB Load Benchmark - All Types (Downstream Consumer Simulation)                                   ║
╠══════════╦═══════════╦═══════════╦════════════╦═══════════╦════════════╦════════════════╦══════════════╦═════════════╣
║  Type    ║  Msg KB   ║  Rmt KB   ║  Tx KB     ║ Total KB  ║ Load (ms)  ║ Peak Off-Heap  ║  Rows/sec    ║  Tx Rows    ║
╠══════════╬═══════════╬═══════════╬════════════╬═══════════╬════════════╬════════════════╬══════════════╬═════════════╣
║  Type A   ║         0 ║         2 ║    301,709 ║   301,712 ║        635 ║    121,576,854 ║    1,574,804 ║   1,000,000 ║
║  Type B   ║         0 ║         2 ║    301,600 ║   301,604 ║        636 ║    121,541,462 ║    1,572,330 ║   1,000,000 ║
║  Type C   ║         0 ║   205,125 ║    301,654 ║   506,780 ║      1,291 ║    121,511,222 ║    1,549,186 ║   1,000,000 ║
║  Type D   ║         0 ║         2 ║         61 ║        65 ║         17 ║         70,486 ║       11,882 ║         200 ║
║  Type E   ║         0 ║         2 ║         61 ║        65 ║         15 ║         70,486 ║       13,466 ║         200 ║
║  Type F   ║         0 ║         2 ║    604,501 ║   604,504 ║      1,255 ║    121,904,534 ║    1,593,626 ║   2,000,000 ║
║  Type G   ║         0 ║         2 ║  1,210,086 ║ 1,210,090 ║      2,129 ║    121,904,574 ║    1,878,816 ║   4,000,000 ║
╚══════════╩═══════════╩═══════════╩════════════╩═══════════╩════════════╩════════════════╩══════════════╩═════════════╝

  Peak Off-Heap = Arrow allocator peak off-heap bytes during ArrowIpc.load (ArrowStreamReader batches)
```

- Downstream consumers load **1 M rows from Arrow IPC files in 635–1,291 ms** and **4M rows in 2,129 ms** using `ArrowIpc.load()` (C Data Interface, no extension)
- **Type F (2M rows)** loads in 1,255 ms; **Type G (4M rows)** loads in 2,129 ms — near-linear scaling with row count
- Peak off-heap for large types is ~121–122 MB (one batch of 65k rows per table held transiently during load) — bounded regardless of file size
- `Msg KB` shows 0 because the single-row message table is sub-1 KB (rounds down)

## `pgw-validator` — Validation Benchmark (`ValidationBenchmarkTest`)

**Path:** `.arrow` files → `ArrowIpc.load()` → DuckDB → `ValidationPipeline.standard()` + `StreamingTransactionIteratorValidator`

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║         Validation Stage Benchmark — Arrow File → DuckDB Load + SQL Validation                                              ║
╠══════════╦════════════╦══════════════╦══════════════╦════════════════╦═══════════╦══════════════╦═══════════╣
║  Type    ║ Arrow (KB) ║ DuckDB ms    ║ Validate ms  ║ Peak Off-Heap  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬══════════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  Type A   ║    301,712 ║          768 ║           80 ║    121,576,862 ║ 1,000,000 ║       12,500 ║ ✓ PASSED  ║
║  Type B   ║    301,604 ║          728 ║           82 ║    121,465,798 ║ 1,000,000 ║       12,195 ║ ✓ PASSED  ║
║  Type C   ║    506,780 ║        1,336 ║          329 ║    121,511,222 ║ 1,000,000 ║        3,040 ║ ✓ PASSED  ║
║  Type D   ║         65 ║           15 ║            7 ║         70,486 ║       200 ║           29 ║ ✓ PASSED  ║
║  Type E   ║         65 ║           14 ║           11 ║         70,486 ║       200 ║           18 ║ ✗ 3 err   ║
║  Type F   ║    604,504 ║        1,287 ║          128 ║    121,576,862 ║ 2,000,000 ║       15,625 ║ ✓ PASSED  ║
║  Type G   ║  1,210,090 ║        1,899 ║          250 ║    121,970,078 ║ 4,000,000 ║       16,000 ║ ✓ PASSED  ║
╠══════════╬════════════╬══════════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL    ║  2,924,820 ║        6,047 ║          887 ║    121,970,078 ║ 9,000,400 ║       10,147 ║ —         ║
╚══════════╩════════════╩══════════════╩══════════════╩════════════════╩═══════════╩══════════════╩═══════════╝

  DuckDB ms      = time for ArrowIpc.load() per file (ArrowStreamReader → registerArrowStream → CREATE TABLE AS SELECT)
  Validate ms    = time for ValidationPipeline.standard() — SQL validators running in parallel virtual threads
  Peak Off-Heap  = Arrow allocator peak off-heap bytes during the ArrowIpc.load phase
  rows/ms (val)  = transaction row scan throughput during SQL validation

╔══════════════════════════════════════════════════════════════════════╗
║   Streaming Iteration Benchmark — StreamingTransactionIteratorValidator ║
╠══════════╦═══════════╦════════════════╦══════════════╣
║  Type    ║  Tx Rows  ║ Streaming ms   ║ rows/ms (str)║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  Type A   ║ 1,000,000 ║          7,330 ║          136 ║
║  Type B   ║ 1,000,000 ║          7,429 ║          135 ║
║  Type C   ║ 1,000,000 ║          7,294 ║          137 ║
║  Type D   ║       200 ║              2 ║          100 ║
║  Type E   ║       200 ║              2 ║          100 ║
║  Type F   ║ 2,000,000 ║         14,781 ║          135 ║
║  Type G   ║ 4,000,000 ║         29,558 ║          135 ║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  TOTAL    ║ 9,000,400 ║         66,396 ║          136 ║
╚══════════╩═══════════╩════════════════╩══════════════╝

  Streaming ms   = time for StreamingTransactionIteratorValidator to iterate all rows and
                   map each into a Transaction POJO, checking instructedAmount > 0
  rows/ms (str)  = transaction row streaming throughput (query + result fetch + object mapping + check)

  ► Grand Total Streaming Time (all types A–G): 66,396 ms to iterate through 9,000,400 transaction rows
```

- **TOTAL: 6,047 ms load + 887 ms SQL validation** across 9,000,400 rows (7 types combined)
- **Type F (2M rows):** loads in 1,287 ms, validates in 128 ms (15,625 rows/ms) — ✓ PASSED
- **Type G (4M rows):** loads in 1,899 ms, validates in 250 ms (16,000 rows/ms) — ✓ PASSED
- Type C is slowest to validate (329 ms) because it has 1M remittances to JOIN
- Type E correctly reports **3 control-sum errors** (2 remittance-level + 1 message-level)
- Peak off-heap ~122 MB for large types (bounded to one 65k-row batch per table at a time)

### Streaming Iteration Observations (`StreamingTransactionIteratorValidator`)

- **~135–137 rows/ms** for large types (A, B, C, F, G) — consistent and linear
- **Streaming 1M rows takes ~7,300 ms** vs **80 ms for SQL validation** — ~91× slower
- **Grand total: 66,396 ms to iterate through 9,000,400 transaction rows across all 7 types**
- Confirms that row-by-row JDBC streaming through DuckDB has significant overhead compared to server-side SQL aggregation
- This is the expected cost for use-cases that need per-record inspection (e.g., external API calls per transaction)
- The `Transaction` POJO is materialized for every row: full `ResultSet` column extraction + object allocation included in the measured time

---

---

# Full Test Run Summary

```
══════════════════════════════════════════════════════════════
  pgw-ingestor
══════════════════════════════════════════════════════════════
  ParsePipelineTest          :  4 tests — PASS
  StreamingPipelineTest      :  4 tests — PASS  (ArrowIpc export + load round-trip)
  MemoryLeakVerificationTest :  3 tests — PASS  (103 iterations, 0 bytes leaked)
  IngestionBenchmarkTest     :  1 test  — PASS  (Types A–G, peak ~52 MB off-heap, 4M rows in ~60s)
  ─────────────────────────────────────────────
  Subtotal                   : 12 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  pgw-validator
══════════════════════════════════════════════════════════════
  ValidationTest             :  2 tests — PASS  (Type D passes, Type E fails correctly)
  ArrowFileLoadBenchmarkTest :  1 test  — PASS  (4M rows loaded in 2,129 ms, peak ~122 MB)
  ValidationBenchmarkTest    :  1 test  — PASS  (load 6,047 ms + validate 887 ms + streaming 66,396 ms total for 9,000,400 rows, peak ~122 MB)
  ─────────────────────────────────────────────
  Subtotal                   :  4 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  TOTAL                      : 16 tests — BUILD SUCCESS
══════════════════════════════════════════════════════════════
```


