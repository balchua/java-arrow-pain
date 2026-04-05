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
# All ingestor tests (< 10 s)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor

# Individual test classes
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor -Dtest=ParsePipelineTest

MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor -Dtest=StreamingPipelineTest

MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor -Dtest=MemoryLeakVerificationTest
```

## `pgw-ingestor` Test Classes

| Test Class | What It Tests | Tests |
|-----------|--------------|------:|
| `ParsePipelineTest` | StAX parser correctness: row counts, field values, edge cases | 4 |
| `StreamingPipelineTest` | Memory footprint; DuckDB row counts; `ArrowIpc.export` + `ArrowIpc.load` round-trip | 4 |
| `MemoryLeakVerificationTest` | 50-iteration streaming parse stress test — zero bytes leaked | 3 |
| **Total** | | **11** |

## `pgw-ingestor` Test Results

```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0  -- ParsePipelineTest
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0  -- StreamingPipelineTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0  -- MemoryLeakVerificationTest

pgw-ingestor  :  11 tests — BUILD SUCCESS  (5.6 s)
```

## `pgw-ingestor` — XML → DuckDB Ingestion + Arrow Export (Types A–E)

**Path:** XML → StAX streaming parse → `StreamingBatchConsumer` → DuckDB →
`ArrowIpc.export()` (C Data Interface) → `.arrow` files

| Type | Structure | XML (MB) | Arrow (MB) | Savings | Tx Rows |
|------|-----------|----------|------------|---------|---------|
| A | 1 PmtInf × 1M TxInf (fat batch) | 516 | ~295 | ~43% | 1,000,000 |
| B | 2 PmtInf × 500K TxInf | 516 | ~295 | ~43% | 1,000,000 |
| C | 1M PmtInf × 1 TxInf (many small) | 888 | ~495 | ~44% | 1,000,000 |
| D | 2 PmtInf × 100 TxInf (valid) | <1 | <1 | — | 200 |
| E | 2 PmtInf × 100 TxInf (invalid CtrlSum) | <1 | <1 | — | 200 |

> Types A–C are generated on demand (large files); Types D–E are generated automatically
> by every `mvn test` run of `pgw-ingestor`. Arrow files are cached across runs.

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
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║           Arrow File -> DuckDB Load Benchmark - All Types (Downstream Consumer Simulation)               ║
╠══════════╦═══════════╦═══════════╦════════════╦═══════════╦════════════╦══════════════╦═════════════╣
║  Type    ║  Msg KB   ║  Rmt KB   ║  Tx KB     ║ Total KB  ║ Load (ms)  ║  Rows/sec    ║  Tx Rows    ║
╠══════════╬═══════════╬═══════════╬════════════╬═══════════╬════════════╬══════════════╬═════════════╣
║  Type A   ║         0 ║         2 ║    301,709 ║   301,712 ║        598 ║    1,672,242 ║   1,000,000 ║
║  Type B   ║         0 ║         2 ║    301,600 ║   301,604 ║        588 ║    1,700,683 ║   1,000,000 ║
║  Type C   ║         0 ║   205,125 ║    301,654 ║   506,780 ║      1,176 ║    1,700,680 ║   1,000,000 ║
║  Type D   ║         0 ║         2 ║         61 ║        65 ║         16 ║       12,625 ║         200 ║
║  Type E   ║         0 ║         2 ║         61 ║        65 ║         23 ║        8,782 ║         200 ║
╚══════════╩═══════════╩═══════════╩════════════╩═══════════╩════════════╩══════════════╩═════════════╝
```

- Downstream consumers load **1 M rows from Arrow IPC files in 588–1,176 ms** using `ArrowIpc.load()` (C Data Interface, no extension)
- `Msg KB` shows 0 because the single-row message table is sub-1 KB (rounds down)

## `pgw-validator` — Validation Benchmark (`ValidationBenchmarkTest`)

**Path:** `.arrow` files → `ArrowIpc.load()` → DuckDB → `ValidationPipeline.standard()`

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════╗
║         Validation Stage Benchmark — Arrow File → DuckDB Load + SQL Validation                      ║
╠══════════╦════════════╦══════════════╦══════════════╦═══════════╦══════════════╦═══════════╣
║  Type    ║ Arrow (KB) ║ DuckDB ms    ║ Validate ms  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬══════════════╬══════════════╬═══════════╬══════════════╬═══════════╣
║  Type A   ║    301,712 ║          602 ║           74 ║ 1,000,000 ║       13,514 ║ ✓ PASSED  ║
║  Type B   ║    301,604 ║          579 ║           85 ║ 1,000,000 ║       11,765 ║ ✓ PASSED  ║
║  Type C   ║    506,780 ║        1,168 ║          325 ║ 1,000,000 ║        3,077 ║ ✓ PASSED  ║
║  Type D   ║         65 ║           15 ║            6 ║       200 ║           33 ║ ✓ PASSED  ║
║  Type E   ║         65 ║           14 ║            7 ║       200 ║           29 ║ ✗ 3 err   ║
╠══════════╬════════════╬══════════════╬══════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL    ║  1,110,226 ║        2,378 ║          497 ║ 3,000,400 ║        6,037 ║ —         ║
╚══════════╩════════════╩══════════════╩══════════════╩═══════════╩══════════════╩═══════════╝
```

- **DuckDB ms** = time for `ArrowIpc.load()` per file (ArrowStreamReader → registerArrowStream → CREATE TABLE AS SELECT)
- **Validate ms** = time for `ValidationPipeline.standard()` — 4 SQL validators running in parallel virtual threads
- **TOTAL: 2,378 ms load + 497 ms validation** across 3,000,400 rows (5 types combined)
- Type C is slowest to validate (325 ms) because it has 1M remittances to JOIN
- Type E correctly reports **3 control-sum errors** (2 remittance-level + 1 message-level)

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
  ─────────────────────────────────────────────
  Subtotal                   : 11 tests — BUILD SUCCESS  (5.6 s)

══════════════════════════════════════════════════════════════
  pgw-validator
══════════════════════════════════════════════════════════════
  ValidationTest             :  2 tests — PASS  (Type D passes, Type E fails correctly)
  ArrowFileLoadBenchmarkTest :  1 test  — PASS  (1M rows loaded in 588–1,176 ms)
  ValidationBenchmarkTest    :  1 test  — PASS  (load 2,378 ms + validate 497 ms)
  ─────────────────────────────────────────────
  Subtotal                   :  4 tests — BUILD SUCCESS  (01:41 min)

══════════════════════════════════════════════════════════════
  TOTAL                      : 15 tests — BUILD SUCCESS  (01:47 min)
══════════════════════════════════════════════════════════════
```


