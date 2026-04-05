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

# Full suite — both modules (all 15 tests, ~2 min on first run)
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

# Ingestion benchmark only (generates Types A–C on first run, ~1.5 min)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor -Dtest=IngestionBenchmarkTest
```

## `pgw-ingestor` Test Classes

| Test Class | What It Tests | Tests |
|-----------|--------------|------:|
| `ParsePipelineTest` | StAX parser correctness: row counts, field values, edge cases | 4 |
| `StreamingPipelineTest` | Memory footprint; DuckDB row counts; `ArrowIpc.export` + `ArrowIpc.load` round-trip | 4 |
| `MemoryLeakVerificationTest` | 50-iteration streaming parse stress test — zero bytes leaked | 3 |
| `IngestionBenchmarkTest` | XML → DuckDB INSERT → Arrow export benchmark for Types A–E (parse ms, export ms, peak memory) | 1 |
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
║  Type A  ║   734.4  ║      15,558  ║       599  ║   31,309,824   ║    294.64 ║ 1,000,000 ║
║  Type B  ║   734.3  ║      13,535  ║       558  ║   31,309,824   ║    294.54 ║ 1,000,000 ║
║  Type C  ║ 1,295.1  ║      29,739  ║       916  ║   52,101,120   ║    494.90 ║ 1,000,000 ║
║  Type D  ║     0.1  ║          12  ║         5  ║    1,785,856   ║      0.06 ║       200 ║
║  Type E  ║     0.1  ║          10  ║         4  ║    1,785,856   ║      0.06 ║       200 ║
╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣
║  TOTAL   ║ 2,764.1  ║      58,854  ║     2,082  ║   52,101,120   ║  1,084.21 ║ 3,000,400 ║
╚══════════╩══════════╩══════════════╩════════════╩════════════════╩════════════╩═══════════╝

  XML (MB)       = source XML file size on disk
  Parse+Ins ms   = StAX streaming parse + StreamingBatchConsumer INSERT into DuckDB
  Export ms      = ArrowIpc.export() for all 3 tables (C Data Interface, no extension)
  Peak Off-Heap  = peak Arrow allocator off-heap bytes during parse+insert phase
  Arrow (MB)     = combined size of the 3 exported .arrow files on disk
  Tx Rows        = transaction rows ingested
```

**Key findings:**
- Type C (1M PmtInf × 1 TxInf) is the slowest to ingest (29,739 ms) due to 1M separate `INSERT INTO … SELECT *` batch calls — one per remittance group
- Types A and B are ~2× faster because fewer, larger batches (1–2 `PmtInf` groups → ~16 batches of 65k rows each)
- Arrow export is fast in all cases (599–916 ms for 1M rows) — bounded to one 65k-row batch at a time off-heap
- Peak off-heap stays bounded to one batch per table (~30–52 MB total), not proportional to XML file size

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
| `ArrowFileLoadBenchmarkTest` | Arrow file → DuckDB load time only (no validation), all 5 types | 1 |
| `ValidationBenchmarkTest` | Arrow → DuckDB load time **+** SQL validation time, separated, all 5 types | 1 |
| **Total** | | **4** |

## `pgw-validator` Test Results

```
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0  -- ValidationTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ArrowFileLoadBenchmarkTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ValidationBenchmarkTest

pgw-validator :   4 tests — BUILD SUCCESS  (01:41 min, includes large file generation)
```

## `pgw-validator` — Arrow File → DuckDB Load Benchmark (`ArrowFileLoadBenchmarkTest`)

**Path:** pre-exported `.arrow` files → `ArrowIpc.load()` → DuckDB `CREATE TABLE AS SELECT`

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║           Arrow File -> DuckDB Load Benchmark - All Types (Downstream Consumer Simulation)                                   ║
╠══════════╦═══════════╦═══════════╦════════════╦═══════════╦════════════╦════════════════╦══════════════╦═════════════╣
║  Type    ║  Msg KB   ║  Rmt KB   ║  Tx KB     ║ Total KB  ║ Load (ms)  ║ Peak Off-Heap  ║  Rows/sec    ║  Tx Rows    ║
╠══════════╬═══════════╬═══════════╬════════════╬═══════════╬════════════╬════════════════╬══════════════╬═════════════╣
║  Type A   ║         0 ║         2 ║    301,709 ║   301,712 ║      1,662 ║    121,576,854 ║      601,685 ║   1,000,000 ║
║  Type B   ║         0 ║         2 ║    301,600 ║   301,604 ║        754 ║    121,566,686 ║    1,326,262 ║   1,000,000 ║
║  Type C   ║         0 ║   205,125 ║    301,654 ║   506,780 ║      1,617 ║    121,511,222 ║    1,236,858 ║   1,000,000 ║
║  Type D   ║         0 ║         2 ║         61 ║        65 ║         21 ║         70,486 ║        9,619 ║         200 ║
║  Type E   ║         0 ║         2 ║         61 ║        65 ║         18 ║         70,486 ║       11,222 ║         200 ║
╚══════════╩═══════════╩═══════════╩════════════╩═══════════╩════════════╩════════════════╩══════════════╩═════════════╝

  Peak Off-Heap = Arrow allocator peak off-heap bytes during ArrowIpc.load (ArrowStreamReader batches)
```

- Downstream consumers load **1 M rows from Arrow IPC files in 754–1,662 ms** using `ArrowIpc.load()` (C Data Interface, no extension)
- Peak off-heap for large types is ~121 MB (one batch of 65k rows per table held transiently during load)
- `Msg KB` shows 0 because the single-row message table is sub-1 KB (rounds down)

## `pgw-validator` — Validation Benchmark (`ValidationBenchmarkTest`)

**Path:** `.arrow` files → `ArrowIpc.load()` → DuckDB → `ValidationPipeline.standard()`

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║         Validation Stage Benchmark — Arrow File → DuckDB Load + SQL Validation                                              ║
╠══════════╦════════════╦══════════════╦══════════════╦════════════════╦═══════════╦══════════════╦═══════════╣
║  Type    ║ Arrow (KB) ║ DuckDB ms    ║ Validate ms  ║ Peak Off-Heap  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬══════════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  Type A   ║    301,712 ║        1,688 ║           98 ║    121,576,854 ║ 1,000,000 ║       10,204 ║ ✓ PASSED  ║
║  Type B   ║    301,604 ║          771 ║           90 ║    121,566,686 ║ 1,000,000 ║       11,111 ║ ✓ PASSED  ║
║  Type C   ║    506,780 ║        1,589 ║          328 ║    121,511,222 ║ 1,000,000 ║        3,049 ║ ✓ PASSED  ║
║  Type D   ║         65 ║           19 ║            7 ║         70,486 ║       200 ║           29 ║ ✓ PASSED  ║
║  Type E   ║         65 ║           18 ║           10 ║         70,486 ║       200 ║           20 ║ ✗ 3 err   ║
╠══════════╬════════════╬══════════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL    ║  1,110,226 ║        4,085 ║          533 ║    121,576,854 ║ 3,000,400 ║        5,629 ║ —         ║
╚══════════╩════════════╩══════════════╩══════════════╩════════════════╩═══════════╩══════════════╩═══════════╝

  DuckDB ms      = time for ArrowIpc.load() per file (ArrowStreamReader → registerArrowStream → CREATE TABLE AS SELECT)
  Validate ms    = time for ValidationPipeline.standard() — SQL validators running in parallel virtual threads
  Peak Off-Heap  = Arrow allocator peak off-heap bytes during the ArrowIpc.load phase
  rows/ms (val)  = transaction row scan throughput during SQL validation
```

- **TOTAL: 4,085 ms load + 533 ms validation** across 3,000,400 rows (5 types combined)
- Type C is slowest to validate (328 ms) because it has 1M remittances to JOIN
- Type E correctly reports **3 control-sum errors** (2 remittance-level + 1 message-level)
- Peak off-heap ~121 MB for large types (bounded to one 65k-row batch per table at a time)

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
  IngestionBenchmarkTest     :  1 test  — PASS  (XML→DuckDB→Arrow export, peak ~52 MB off-heap)
  ─────────────────────────────────────────────
  Subtotal                   : 12 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  pgw-validator
══════════════════════════════════════════════════════════════
  ValidationTest             :  2 tests — PASS  (Type D passes, Type E fails correctly)
  ArrowFileLoadBenchmarkTest :  1 test  — PASS  (1M rows loaded in 754–1,662 ms, peak ~121 MB)
  ValidationBenchmarkTest    :  1 test  — PASS  (load 4,085 ms + validate 533 ms, peak ~121 MB)
  ─────────────────────────────────────────────
  Subtotal                   :  4 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  TOTAL                      : 16 tests — BUILD SUCCESS
══════════════════════════════════════════════════════════════
```


