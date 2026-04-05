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

`StreamingBatchConsumer` is the only sink — it inserts into DuckDB via Arrow C Data
Interface. After parsing, `ArrowIpc.export()` (or `App.java`) uses DuckDB's built-in
`DuckDBResultSet.arrowExportStream` to export three Arrow IPC stream files without any extension.

---

## How to Run Tests

```bash
# No extension installation needed — ArrowIpc uses the C Data Interface built into DuckDB JDBC.

# 1. Build
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn clean package -DskipTests

# 2. Fast tests (< 30 s) — Types D + E only, table output in console
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor \
  -Dtest="ParsePipelineTest,StreamingPipelineTest,MemoryLeakVerificationTest"

# Also run the domain validation correctness tests
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-validator --also-make \
  -Dtest="ValidationTest"

# 3. Arrow→DuckDB load benchmark (all 5 types, table in console)
#    First run generates large XML files (Types A–C: ~516–888 MB); cached on subsequent runs.
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator --also-make \
  -Dtest="ArrowFileLoadBenchmarkTest"

# 4. Validation benchmark (all 5 types, DuckDB load + SQL validation, table in console)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator --also-make \
  -Dtest="ValidationBenchmarkTest"

# 5. Full suite (all 15 tests, ~2 min on first run due to large XML generation)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor,pgw-validator

# 6. Standardized before/after comparison (saves timestamped log to test-results/)
./run_validation_tests.sh
```

> Both module POMs set `redirectTestOutputToFile=false` so the benchmark tables
> (printed via `System.out`) appear directly in the Maven console output.

---

## Test Suite — Module Breakdown

| Module | Test Class | Type | Tests |
|--------|-----------|------|------:|
| `pgw-ingestor` | `ParsePipelineTest` | Correctness | 4 |
| `pgw-ingestor` | `StreamingPipelineTest` | Correctness + ArrowIpc export/load | 4 |
| `pgw-ingestor` | `MemoryLeakVerificationTest` | Memory safety | 3 |
| `pgw-validator` | `ValidationTest` | Domain validation correctness | 2 |
| `pgw-validator` | `ArrowFileLoadBenchmarkTest` | Performance | 1 |
| `pgw-validator` | `ValidationBenchmarkTest` | Performance | 1 |
| **Total** | | | **15** |

---

## XML → DuckDB Ingestion + Arrow Export — Types A–E

**Path:** XML → StAX streaming parse → `StreamingBatchConsumer` → DuckDB →
`ArrowIpc.export()` (C Data Interface) → `.arrow` files

| Type | XML (MB) | Arrow (MB) | Savings | Parse+Export (s) | Tx Rows |
|------|----------|------------|---------|-----------------|---------|
| A — 1×1M txns | 516 | ~295 | ~43% | ~90 | 1,000,000 |
| B — 2×500K txns | 516 | ~295 | ~43% | ~90 | 1,000,000 |
| C — 1M×1 txns | 888 | ~495 | ~44% | ~90 | 1,000,000 |
| D — 2×100 (valid) | <1 | <1 | — | <1 | 200 |
| E — 2×100 (invalid CtrlSum) | <1 | <1 | — | <1 | 200 |

> Timings include XML generation (first run) + parse + Arrow export.
> Arrow files are cached; subsequent benchmark runs skip generation.

---

## Arrow File → DuckDB Load Benchmark (`ArrowFileLoadBenchmarkTest`)

**Path:** pre-exported `.arrow` files → `ArrowIpc.load()` → DuckDB `CREATE TABLE AS SELECT`

Actual results from `mvn test -pl pgw-validator -Dtest=ArrowFileLoadBenchmarkTest`:

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

> Downstream consumers load **1 M rows from Arrow IPC files in 588–1,176 ms**
> using `ArrowIpc.load()` (C Data Interface, no extension).

---

## Validation Benchmark (`ValidationBenchmarkTest`)

**Path:** `.arrow` files → `ArrowIpc.load()` → DuckDB → `ValidationPipeline.standard()`

Actual results from `mvn test -pl pgw-validator -Dtest=ValidationBenchmarkTest`:

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

- **DuckDB ms** = time for `ArrowIpc.load()` (ArrowStreamReader → registerArrowStream → CREATE TABLE) per file
- **Validate ms** = time for `ValidationPipeline.standard()` — 4 SQL validators in parallel virtual threads
- **TOTAL: 2,378 ms load + 497 ms validation** across 3,000,400 rows (5 types combined)
- Type E correctly reports **3 control-sum errors** (2 remittance-level + 1 message-level)

---

## Memory Leak Verification (`MemoryLeakVerificationTest`)

Runs Type D and E 50× each through the streaming parse → DuckDB pipeline,
checking that `allocator.getAllocatedMemory() == 0` after every iteration.

| Scenario | Iterations | Bytes Leaked |
|----------|----------:|------------:|
| Type D (valid, streaming) | 50 | **0** |
| Type E (invalid CtrlSum, streaming) | 50 | **0** |
| Type D (short) | 3 | **0** |

✅ **Zero bytes leaked across all 103 iterations.**

---

## Full Test Run Summary

```
[INFO] Tests run: 4, Failures: 0, Errors: 0  -- ParsePipelineTest       (pgw-ingestor)
[INFO] Tests run: 4, Failures: 0, Errors: 0  -- StreamingPipelineTest   (pgw-ingestor)
[INFO] Tests run: 3, Failures: 0, Errors: 0  -- MemoryLeakVerificationTest (pgw-ingestor)
[INFO] Tests run: 2, Failures: 0, Errors: 0  -- ValidationTest          (pgw-validator)
[INFO] Tests run: 1, Failures: 0, Errors: 0  -- ArrowFileLoadBenchmarkTest (pgw-validator)
[INFO] Tests run: 1, Failures: 0, Errors: 0  -- ValidationBenchmarkTest (pgw-validator)

pgw-ingestor  : 11 tests — BUILD SUCCESS
pgw-validator :  4 tests — BUILD SUCCESS
Total         : 15 tests — BUILD SUCCESS  (01:47 min)
```

