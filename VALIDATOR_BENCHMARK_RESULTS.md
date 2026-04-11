# PGW — Validator Benchmark Results

Validation modules: `pgw-common` + `pgw-domain` + `pgw-validator` + `pgw-validator-pure-arrow`

See also [INGESTOR_BENCHMARK_RESULTS.md](INGESTOR_BENCHMARK_RESULTS.md) for ingestion benchmark results.

**Environment:** Java 25 (Temurin 25.0.2), Maven 3.9, `-Xmx4g`,
`--add-opens=java.base/java.nio=ALL-UNNAMED`

**No extensions required.** Arrow export and import use DuckDB's built-in C Data Interface
(`DuckDBResultSet.arrowExportStream` / `registerArrowStream`) — works in air-gapped environments.

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
| `ArrowFileLoadBenchmarkTest` | Arrow file → DuckDB load time only (no validation), all 10 types A–J | 1 |
| `ValidationBenchmarkTest` | Arrow → DuckDB load time **+** SQL validation time, separated, all 10 types A–J | 1 |
| **Total** | | **4** |

## `pgw-validator` Test Results

```
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0  -- ValidationTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ArrowFileLoadBenchmarkTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ValidationBenchmarkTest

pgw-validator :   4 tests — BUILD SUCCESS
```

## `pgw-validator` — Arrow File → DuckDB Load Benchmark (`ArrowFileLoadBenchmarkTest`)

Results from actual test run (2026-04-11, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║           Arrow File -> DuckDB Load Benchmark - All Types (Downstream Consumer Simulation)                                   ║
╠══════════╦═══════════╦═══════════╦════════════╦═══════════╦════════════╦════════════════╦══════════════╦═════════════╣
║  Type    ║  Msg KB   ║  Rmt KB   ║  Tx KB     ║ Total KB  ║ Load (ms)  ║ Peak Off-Heap  ║  Rows/sec    ║  Tx Rows    ║
╠══════════╬═══════════╬═══════════╬════════════╬═══════════╬════════════╬════════════════╬══════════════╬═════════════╣
║  Type A   ║         0 ║         2 ║    301,709 ║   301,712 ║        594 ║    121,576,862 ║    1,683,503 ║   1,000,000 ║
║  Type B   ║         0 ║         2 ║    301,600 ║   301,604 ║        599 ║    121,552,894 ║    1,669,452 ║   1,000,000 ║
║  Type C   ║         0 ║   205,125 ║    301,654 ║   506,780 ║      1,249 ║    121,511,222 ║    1,601,281 ║   1,000,000 ║
║  Type D   ║         0 ║         2 ║         61 ║        65 ║         14 ║         70,486 ║       14,428 ║         200 ║
║  Type E   ║         0 ║         2 ║         61 ║        65 ║         16 ║         70,486 ║       12,625 ║         200 ║
║  Type F   ║         0 ║         2 ║    604,501 ║   604,504 ║      1,263 ║    121,576,862 ║    1,583,532 ║   2,000,000 ║
║  Type G   ║         0 ║         2 ║  1,210,086 ║ 1,210,090 ║      1,982 ║    141,889,390 ║    2,018,163 ║   4,000,000 ║
║  Type H   ║         0 ║         4 ║        598 ║       604 ║         19 ║      1,053,750 ║      105,789 ║       2,000 ║
║  Type I   ║         0 ║         3 ║        599 ║       603 ║         15 ║      1,053,750 ║      133,666 ║       2,000 ║
║  Type J   ║         0 ║         2 ║          2 ║         5 ║         17 ║          5,944 ║          117 ║           1 ║
╚══════════╩═══════════╩═══════════╩════════════╩═══════════╩════════════╩════════════════╩══════════════╩═════════════╝

  Peak Off-Heap = Arrow allocator off-heap bytes after loading all 3 tables into DuckDB
```

## `pgw-validator` — DuckDB Load + SQL Validation Benchmark (`ValidationBenchmarkTest`)

Results from actual test run (2026-04-11, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║         Validation Stage Benchmark — Arrow File → DuckDB Load + SQL Validation                                              ║
╠══════════╦════════════╦══════════════╦══════════════╦════════════════╦═══════════╦══════════════╦═══════════╣
║  Type    ║ Arrow (KB) ║ DuckDB ms    ║ Validate ms  ║ Peak Off-Heap  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬══════════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  Type A   ║    301,712 ║          627 ║           86 ║    121,576,862 ║ 1,000,000 ║       11,628 ║ ✓ PASSED  ║
║  Type B   ║    301,604 ║          611 ║           83 ║    121,511,854 ║ 1,000,000 ║       12,048 ║ ✓ PASSED  ║
║  Type C   ║    506,780 ║        1,187 ║          318 ║    121,511,222 ║ 1,000,000 ║        3,145 ║ ✓ PASSED  ║
║  Type D   ║         65 ║           14 ║            6 ║         70,486 ║       200 ║           33 ║ ✓ PASSED  ║
║  Type E   ║         65 ║           14 ║           10 ║         70,486 ║       200 ║           20 ║ ✗ 3 err   ║
║  Type F   ║    604,504 ║        1,143 ║          135 ║    121,904,566 ║ 2,000,000 ║       14,815 ║ ✓ PASSED  ║
║  Type G   ║  1,210,090 ║        2,026 ║          253 ║    121,970,078 ║ 4,000,000 ║       15,810 ║ ✓ PASSED  ║
║  Type H   ║        604 ║           16 ║            7 ║      1,053,750 ║     2,000 ║          286 ║ ✓ PASSED  ║
║  Type I   ║        603 ║           16 ║            7 ║      1,053,750 ║     2,000 ║          286 ║ ✓ PASSED  ║
║  Type J   ║          5 ║           13 ║            6 ║          5,944 ║         1 ║            0 ║ ✓ PASSED  ║
╠══════════╬════════════╬══════════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL    ║  2,926,032 ║        5,667 ║          911 ║    121,970,078 ║ 9,004,401 ║        9,884 ║ —         ║
╚══════════╩════════════╩══════════════╩══════════════╩════════════════╩═══════════╩══════════════╩═══════════╝

  Arrow (KB)     = combined size of the 3 .arrow IPC files on disk
  DuckDB ms      = time to load Arrow IPC files into DuckDB via ArrowIpc.load() (C Data Interface)
  Validate ms    = time to run ValidationPipeline.standard() via SQL (IBAN MOD-97, BIC, ControlSum, …)
  Peak Off-Heap  = max Arrow allocator off-heap bytes after loading (stays ~120 MB for large types)
  rows/ms (val)  = transaction row throughput during SQL validation phase
```

## `pgw-validator` — Streaming Iteration Benchmark (`StreamingTransactionIteratorValidator`)

```
╔══════════════════════════════════════════════════════════════════════╗
║   Streaming Iteration Benchmark — StreamingTransactionIteratorValidator ║
╠══════════╦═══════════╦════════════════╦══════════════╣
║  Type    ║  Tx Rows  ║ Streaming ms   ║ rows/ms (str)║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  Type A   ║ 1,000,000 ║          7,150 ║          140 ║
║  Type B   ║ 1,000,000 ║          7,038 ║          142 ║
║  Type C   ║ 1,000,000 ║          7,012 ║          143 ║
║  Type D   ║       200 ║              2 ║          100 ║
║  Type E   ║       200 ║              2 ║          100 ║
║  Type F   ║ 2,000,000 ║         14,221 ║          141 ║
║  Type G   ║ 4,000,000 ║         28,292 ║          141 ║
║  Type H   ║     2,000 ║             15 ║          133 ║
║  Type I   ║     2,000 ║             15 ║          133 ║
║  Type J   ║         1 ║              0 ║            1 ║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  TOTAL    ║ 9,004,401 ║         63,747 ║          141 ║
╚══════════╩═══════════╩════════════════╩══════════════╝

  Streaming ms   = time for StreamingTransactionIteratorValidator to iterate rows via JDBC cursor
  ► Grand Total: 63,747 ms for 9,004,401 rows  ≈ 141 rows/ms (DuckDB JDBC streaming)
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
     ↓  [ArrowPaymentRepositoryLoader — ArrowStreamReader + TransferPair (zero-copy buffer ownership transfer)]
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
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx8g" \
  mvn test -pl pgw-validator-pure-arrow

# Correctness tests only (fast)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-validator-pure-arrow -Dtest=ArrowValidationTest

# Benchmark only (all types A–J)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx8g" \
  mvn test -pl pgw-validator-pure-arrow -Dtest=ArrowValidationBenchmarkTest
```

## `pgw-validator-pure-arrow` Test Classes

| Test Class | What It Tests | Tests |
|-----------|--------------|------:|
| `ArrowValidationTest` | Correctness: Types D (passes), E (CtrlSum errors), H, J | 4 |
| `ArrowValidationBenchmarkTest` | Arrow ingest + Arrow load + pure-Java validation, all types A–J | 1 |
| **Total** | | **5** |

## `pgw-validator-pure-arrow` Test Results

```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0  -- ArrowValidationTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ArrowValidationBenchmarkTest

pgw-validator-pure-arrow :  5 tests — BUILD SUCCESS
```

## `pgw-validator-pure-arrow` — Benchmark (Types A–J)

Results from actual test run (2026-04-11, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║    Pure-Arrow Validation Benchmark — XML→Arrow + Arrow Load + SQL-equivalent Validation (no DuckDB)                                  ║
╠══════════╦════════════╦═══════════════╦═══════════╦══════════════╦════════════════╦═══════════╦══════════════╦═══════════╣
║  Type    ║ Arrow (KB) ║  Ingest ms    ║  Load ms  ║ Validate ms  ║ Peak Off-Heap  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬═══════════════╬═══════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  Type A   ║    301,712 ║        12,378 ║        63 ║        3,003 ║    485,474,304 ║ 1,000,000 ║          333 ║ ✓ PASSED  ║
║  Type B   ║    301,604 ║        10,699 ║        34 ║        3,029 ║    485,474,304 ║ 1,000,000 ║          330 ║ ✓ PASSED  ║
║  Type C   ║    506,780 ║        17,881 ║       108 ║        6,259 ║    829,685,760 ║ 1,000,000 ║          160 ║ ✓ PASSED  ║
║  Type D   ║         65 ║             7 ║         0 ║            1 ║      1,785,856 ║       200 ║          200 ║ ✓ PASSED  ║
║  Type E   ║         65 ║             5 ║         0 ║            3 ║      1,785,856 ║       200 ║           67 ║ ✗ 3 err   ║
║  Type F   ║    604,504 ║        19,809 ║        66 ║        6,325 ║    939,638,784 ║ 2,000,000 ║          316 ║ ✓ PASSED  ║
║  Type G   ║  1,210,090 ║        39,556 ║       169 ║       12,134 ║  1,878,245,376 ║ 4,000,000 ║          330 ║ ✓ PASSED  ║
║  Type H   ║        604 ║            24 ║         1 ║            7 ║      2,113,536 ║     2,000 ║          286 ║ ✓ PASSED  ║
║  Type I   ║        603 ║            22 ║         1 ║            6 ║      2,113,536 ║     2,000 ║          333 ║ ✓ PASSED  ║
║  Type J   ║          6 ║             2 ║         0 ║            0 ║      1,785,856 ║         1 ║            1 ║ ✓ PASSED  ║
╠══════════╬════════════╬═══════════════╬═══════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL    ║  2,926,033 ║       100,383 ║       442 ║       30,767 ║  1,878,245,376 ║ 9,004,401 ║          293 ║ —         ║
╚══════════╩════════════╩═══════════════╩═══════════╩══════════════╩════════════════╩═══════════╩══════════════╩═══════════╝

  Arrow (KB)     = combined size of the 3 .arrow IPC files on disk
  Ingest ms      = XML → StAX parse → PureArrowBatchConsumer → .arrow files (no DuckDB)
  Load ms        = time to materialise .arrow files into ArrowPaymentRepositoryImpl (zero-copy TransferPair)
  Validate ms    = time to run ValidationPipeline.standard() in pure Java/Arrow (no SQL)
  Peak Off-Heap  = max Arrow allocator off-heap bytes during load+validate phases
                   (in-memory store released before loading — only the loaded repo batches contribute)
  rows/ms (val)  = transaction row scan throughput during validation
```

## `pgw-validator-pure-arrow` — Streaming Iteration (StreamingTransactionIteratorValidator)

```
╔══════════════════════════════════════════════════════════════════════╗
║   Streaming Iteration — StreamingTransactionIteratorValidator         ║
╠══════════╦═══════════╦════════════════╦══════════════╣
║  Type    ║  Tx Rows  ║ Streaming ms   ║ rows/ms (str)║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  Type A   ║ 1,000,000 ║          1,512 ║          661 ║
║  Type B   ║ 1,000,000 ║          1,518 ║          659 ║
║  Type C   ║ 1,000,000 ║          1,626 ║          615 ║
║  Type D   ║       200 ║              0 ║          200 ║
║  Type E   ║       200 ║              0 ║          200 ║
║  Type F   ║ 2,000,000 ║          3,028 ║          661 ║
║  Type G   ║ 4,000,000 ║          6,087 ║          657 ║
║  Type H   ║     2,000 ║              2 ║        1,000 ║
║  Type I   ║     2,000 ║              2 ║        1,000 ║
║  Type J   ║         1 ║              0 ║            1 ║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  TOTAL    ║ 9,004,401 ║         13,775 ║          654 ║
╚══════════╩═══════════╩════════════════╩══════════════╝

  Streaming ms   = time for StreamingTransactionIteratorValidator to iterate all rows
                   directly from Arrow vectors (no SQL, no JDBC cursor)
  ► Grand Total: 13,775 ms for 9,004,401 rows  ≈ 654 rows/ms pure Arrow vector iteration
```

**Key observations:**
- **Same `ValidationPipeline.standard()`** runs unchanged on both backends (polymorphism through `PaymentRepository` interface).
- **Validation results are identical**: both pipelines agree on every pass/fail decision (Type E: 3 CtrlSum errors each).
- **Load is faster** in pure Arrow: zero-copy via `TransferPair` is faster than Arrow C Data Interface round-trip through DuckDB.
- **Pure-Arrow streaming throughput** is ~4.6× faster (654 rows/ms vs 141 rows/ms for DuckDB JDBC cursor).
- **Peak off-heap** for pure-Arrow load+validate: ~485 MB for 1M tx (Type A/B), ~1,878 MB for 4M tx (Type G).
  - DuckDB: ~122 MB for any size (bounded — data held in DuckDB's own memory, not Arrow allocator)

**When to choose each validator backend:**
- **DuckDB validator** (`pgw-validator`) → when memory is constrained (~122 MB Arrow off-heap vs ~485 MB+ for pure Arrow), or when OLAP-scale SQL validation logic is complex (window functions, joins across tables, regex).
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
  pgw-duckdb-helper
══════════════════════════════════════════════════════════════
  (no tests — shared utility module)
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
  ─────────────────────────────────────────────
  Subtotal                          : 11 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  pgw-validator
══════════════════════════════════════════════════════════════
  ValidationTest             :  2 tests — PASS  (Type D passes, Type E fails correctly)
  ArrowFileLoadBenchmarkTest :  1 test  — PASS  (Types A–J, all pass)
  ValidationBenchmarkTest    :  1 test  — PASS  (Types A–J, all pass)
  ─────────────────────────────────────────────
  Subtotal                   :  4 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  pgw-validator-pure-arrow
══════════════════════════════════════════════════════════════
  ArrowValidationTest              :  4 tests — PASS  (D passes, E fails, H & J pass)
  ArrowValidationBenchmarkTest     :  1 test  — PASS  (Types A–J, peak 1.8 MB–1.9 GB off-heap)
  ─────────────────────────────────────────────
  Subtotal                         :  5 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  TOTAL                      : 34 tests — BUILD SUCCESS
══════════════════════════════════════════════════════════════
```

**All 34 tests pass. Benchmark data above is from actual test runs on 2026-04-11 (Java 25 Temurin 25.0.2, -Xmx8g).**
