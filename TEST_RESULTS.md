# PGW — Test Results & Benchmark Report

Multi-module build (`pgw-ingestor` + `pgw-validator`), package root `com.pgw`.

**Environment:** Java 25 (Temurin 25.0.2), Maven 3.9, `-Xmx2g`,
`--add-opens=java.base/java.nio=ALL-UNNAMED`

**Prerequisite:** DuckDB Arrow community extension must be installed once before running:
```bash
bash install-arrow-extension.sh
```

---

## Pipeline Architecture

```
XML → StAX parse → StreamingBatchConsumer → DuckDB (single sink)
                                                  ↓
                                    COPY (SELECT * FROM table)
                                      TO 'file.arrow' (FORMAT arrow)
                                                  ↓
                                         *.arrow files on disk
                                                  ↓  [read_arrow()]
                                    Validator in-process DuckDB
                                                  ↓
                                    SQL validation pipeline
```

`StreamingBatchConsumer` is the only sink — it inserts into DuckDB via Arrow C Data
Interface. After parsing, `App.java` (or the benchmark tests) call DuckDB's native
`COPY TO (FORMAT arrow)` to export three Arrow files.

---

## How to Run Tests

```bash
# 1. Install the DuckDB Arrow extension (once, requires internet)
bash install-arrow-extension.sh

# 2. Build
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn clean package -DskipTests

# 3. Fast tests (< 30 s) — Types D + E only, table output in console
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor,pgw-validator \
  -Dtest="ParsePipelineTest,StreamingPipelineTest,MemoryLeakVerificationTest,ValidationTest"

# 4. Arrow→DuckDB load benchmark (all 5 types, table in console)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-validator --also-make \
  -Dtest="ArrowFileLoadBenchmarkTest"

# 5. Validation benchmark (all 5 types, table in console)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-validator --also-make \
  -Dtest="ValidationBenchmarkTest"

# 6. Full suite
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-ingestor,pgw-validator
```

> Both module POMs set `redirectTestOutputToFile=false` so the benchmark tables
> (printed via `System.out`) appear directly in the Maven console output.

---

## Test Suite — Module Breakdown

| Module | Test Class | Type | Tests |
|--------|-----------|------|------:|
| `pgw-ingestor` | `ParsePipelineTest` | Correctness | 5 |
| `pgw-ingestor` | `StreamingPipelineTest` | Correctness + Arrow export | 4 |
| `pgw-ingestor` | `MemoryLeakVerificationTest` | Memory safety | 3 |
| `pgw-validator` | `ValidationTest` | Domain validation correctness | 2 |
| `pgw-validator` | `ArrowFileLoadBenchmarkTest` | Performance | 1 |
| `pgw-validator` | `ValidationBenchmarkTest` | Performance | 1 |
| **Total** | | | **16** |

> `ParsePipelineTest` and `MemoryLeakVerificationTest` do **not** use DuckDB Arrow export —
> they work with the StAX parser and Arrow C Data Interface only, so they run without
> the arrow extension.
>
> `StreamingPipelineTest`, `ArrowFileLoadBenchmarkTest`, `ValidationBenchmarkTest`, and
> `ValidationTest` all call `DuckDbFactory.newConnection()` which loads the arrow extension,
> so the extension must be installed.

---

## XML → DuckDB Ingestion + Arrow Export — Types A–E

**Path:** XML → StAX streaming parse → `StreamingBatchConsumer` → DuckDB →
`COPY TO (FORMAT arrow)` → `.arrow` files

| Type | XML (MB) | Arrow (MB) | Savings | Parse+Export (s) | Tx Rows |
|------|----------|------------|---------|-----------------|---------|
| A — 1×1M txns | 516 | ~225 | ~56% | ~10 | 1,000,000 |
| B — 2×500K txns | 516 | ~225 | ~56% | ~8 | 1,000,000 |
| C — 1M×1 txns | 888 | ~362 | ~59% | ~22 | 1,000,000 |
| D — 2×100 (valid) | 0.1 | <1 | — | <1 | 200 |
| E — 2×100 (invalid CtrlSum) | 0.1 | <1 | — | <1 | 200 |

> Exact timings vary by machine. Run `ArrowFileLoadBenchmarkTest` for measured results.
> The Arrow export (`COPY TO`) typically adds ~1–2 s on top of the parse time.

---

## Arrow File → DuckDB Load Benchmark (`ArrowFileLoadBenchmarkTest`)

**Path:** pre-exported `.arrow` files → `read_arrow()` → DuckDB `CREATE TABLE AS SELECT`

Console output (run `mvn test -pl pgw-validator -Dtest=ArrowFileLoadBenchmarkTest`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║           Arrow File -> DuckDB Load Benchmark - All Types (Downstream Consumer Simulation)               ║
╠══════════╦═══════════╦═══════════╦════════════╦═══════════╦════════════╦══════════════╦═════════════╣
║  Type    ║  Msg KB   ║  Rmt KB   ║  Tx KB     ║ Total KB  ║ Load (ms)  ║  Rows/sec    ║  Tx Rows    ║
╠══════════╬═══════════╬═══════════╬════════════╬═══════════╬════════════╬══════════════╬═════════════╣
║  Type A   ║       <1  ║       <1  ║   229,803  ║  229,806  ║       ~380 ║  ~2,600,000  ║   1,000,000 ║
║  Type B   ║       <1  ║       <1  ║   229,695  ║  229,698  ║       ~376 ║  ~2,660,000  ║   1,000,000 ║
║  Type C   ║       <1  ║  139,064  ║   231,702  ║  370,767  ║       ~687 ║  ~2,910,000  ║   1,000,000 ║
║  Type D   ║       <1  ║       <1  ║       <1   ║      <1   ║        <5  ║          —   ║         200 ║
║  Type E   ║       <1  ║       <1  ║       <1   ║      <1   ║        <5  ║          —   ║         200 ║
╚══════════╩═══════════╩═══════════╩════════════╩═══════════╩════════════╩══════════════╩═════════════╝
```

> Downstream consumers load 1 M rows from Arrow files in **376–687 ms**
> using DuckDB's native `read_arrow()` function.

---

## Validation Benchmark (`ValidationBenchmarkTest`)

**Path:** `.arrow` files → `read_arrow()` → DuckDB → `ValidationPipeline.standard()`

Console output (run `mvn test -pl pgw-validator -Dtest=ValidationBenchmarkTest`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════╗
║         Validation Stage Benchmark — Arrow File → DuckDB Load + SQL Validation                      ║
╠══════════╦════════════╦══════════════╦══════════════╦═══════════╦══════════════╦═══════════╣
║  Type    ║ Arrow (KB) ║ DuckDB ms    ║ Validate ms  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬══════════════╬══════════════╬═══════════╬══════════════╬═══════════╣
║  Type A  ║    229,806 ║         ~380 ║          ~79 ║ 1,000,000 ║      ~12,658 ║ ✓ PASSED  ║
║  Type B  ║    229,698 ║         ~379 ║          ~79 ║ 1,000,000 ║      ~12,658 ║ ✓ PASSED  ║
║  Type C  ║    370,767 ║         ~714 ║         ~321 ║ 1,000,000 ║       ~3,115 ║ ✓ PASSED  ║
║  Type D  ║         65 ║          <10 ║           <5 ║       200 ║           —  ║ ✓ PASSED  ║
║  Type E  ║         65 ║          <10 ║           <5 ║       200 ║           —  ║ ✗ 3 err   ║
╠══════════╬════════════╬══════════════╬══════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL   ║  1,110,226 ║       ~1,473 ║         ~479 ║ 3,000,400 ║          —   ║     —     ║
╚══════════╩════════════╩══════════════╩══════════════╩═══════════╩══════════════╩═══════════╝
```

- **DuckDB ms** = time for `read_arrow()` + `CREATE TABLE AS SELECT` per table
- **Validate ms** = time for `ValidationPipeline.standard()` — 4 SQL validators in parallel
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

## CI

Tests run automatically on every push/PR via `.github/workflows/ci.yml`:

- **Fast tests** (`ParsePipelineTest`, `StreamingPipelineTest`, `MemoryLeakVerificationTest`,
  `ValidationTest`) — always run, < 30 s
- **Benchmark tests** (`ArrowFileLoadBenchmarkTest`, `ValidationBenchmarkTest`) — run on
  `workflow_dispatch` or when commit message contains `[bench]`

The workflow calls `install-arrow-extension.sh` to install the DuckDB Arrow extension
before running any tests.
