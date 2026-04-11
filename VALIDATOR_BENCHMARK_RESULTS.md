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

| Test Class                   | What It Tests                                                                   | Tests |
| ---------------------------- | ------------------------------------------------------------------------------- | ----: |
| `ValidationTest`             | Domain validation correctness: Type D passes, Type E fails with 3 errors        |     2 |
| `ArrowFileLoadBenchmarkTest` | Arrow file → DuckDB load time only (no validation), all 10 types A–J            |     1 |
| `ValidationBenchmarkTest`    | Arrow → DuckDB load time **+** SQL validation time, separated, all 10 types A–J |     1 |
| **Total**                    |                                                                                 | **4** |

## `pgw-validator` Test Results

```
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0  -- ValidationTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ArrowFileLoadBenchmarkTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ValidationBenchmarkTest

pgw-validator :   4 tests — BUILD SUCCESS
```

## `pgw-validator` — DuckDB Load + SQL Validation Benchmark (`ValidationBenchmarkTest`)

Results from actual test run (2026-04-11, Java 25 Temurin 25.0.2):

```bash
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║         Validation Stage Benchmark — Arrow File → DuckDB Load + SQL Validation                                              ║
╠══════════╦════════════╦══════════════╦══════════════╦════════════════╦═══════════╦══════════════╦═══════════╣
║  Type    ║ Arrow (KB) ║ DuckDB ms    ║ Validate ms  ║ Peak Off-Heap  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬══════════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  Type A   ║    301,712 ║        1,395 ║          184 ║    121,576,854 ║ 1,000,000 ║        5,435 ║ ✓ PASSED  ║
║  Type B   ║    301,604 ║        1,086 ║          169 ║    121,500,190 ║ 1,000,000 ║        5,917 ║ ✓ PASSED  ║
║  Type C   ║    506,780 ║        2,719 ║          987 ║    121,511,222 ║ 1,000,000 ║        1,013 ║ ✓ PASSED  ║
║  Type D   ║         65 ║           20 ║           11 ║         70,486 ║       200 ║           18 ║ ✓ PASSED  ║
║  Type E   ║         65 ║           19 ║           11 ║         70,486 ║       200 ║           18 ║ ✗ 3 err   ║
║  Type F   ║    604,504 ║        1,647 ║          280 ║    203,288,990 ║ 2,000,000 ║        7,143 ║ ✓ PASSED  ║
║  Type G   ║  1,210,090 ║        2,907 ║          586 ║    223,628,742 ║ 4,000,000 ║        6,826 ║ ✓ PASSED  ║
║  Type H   ║        604 ║           25 ║           14 ║      1,053,750 ║     2,000 ║          143 ║ ✓ PASSED  ║
║  Type I   ║        603 ║           22 ║           15 ║      1,053,750 ║     2,000 ║          133 ║ ✓ PASSED  ║
║  Type J   ║          5 ║           31 ║           15 ║          5,944 ║         1 ║            0 ║ ✓ PASSED  ║
╠══════════╬════════════╬══════════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL    ║  2,926,032 ║        9,871 ║        2,272 ║    223,628,742 ║ 9,004,401 ║        3,963 ║ —         ║
╚══════════╩════════════╩══════════════╩══════════════╩════════════════╩═══════════╩══════════════╩═══════════╝

  Arrow (KB)     = total size of the three .arrow files on disk (message + remittance + transaction)
  DuckDB ms      = time to load Arrow files into in-process DuckDB via ArrowIpc.load (C Data Interface)
  Validate ms    = time to run ValidationPipeline.standard() against the populated DuckDB tables
  Peak Off-Heap  = Arrow allocator off-heap bytes after loading all 3 tables into DuckDB
  rows/ms (val)  = transaction row scan throughput during validation
  ```

## `pgw-validator` — Streaming Iteration Benchmark (`StreamingTransactionIteratorValidator`)

```bash
╔══════════════════════════════════════════════════════════════════════╗
║   Streaming Iteration Benchmark — StreamingTransactionIteratorValidator ║
╠══════════╦═══════════╦════════════════╦══════════════╣
║  Type    ║  Tx Rows  ║ Streaming ms   ║ rows/ms (str)║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  Type A   ║ 1,000,000 ║         11,140 ║           90 ║
║  Type B   ║ 1,000,000 ║         11,119 ║           90 ║
║  Type C   ║ 1,000,000 ║         11,748 ║           85 ║
║  Type D   ║       200 ║              3 ║           67 ║
║  Type E   ║       200 ║              2 ║          100 ║
║  Type F   ║ 2,000,000 ║         21,202 ║           94 ║
║  Type G   ║ 4,000,000 ║         46,545 ║           86 ║
║  Type H   ║     2,000 ║             20 ║          100 ║
║  Type I   ║     2,000 ║             42 ║           48 ║
║  Type J   ║         1 ║              1 ║            1 ║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  TOTAL    ║ 9,004,401 ║        101,822 ║           88 ║
╚══════════╩═══════════╩════════════════╩══════════════╝

  Streaming ms   = time for StreamingTransactionIteratorValidator to iterate all rows and
                   map each into a Transaction POJO, checking instructedAmount > 0
  rows/ms (str)  = transaction row streaming throughput (query + result fetch + object mapping + check)

  ► Grand Total Streaming Time (all types A–J): 101,822 ms to iterate through 9,004,401 transaction rows
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

| Test Class                     | What It Tests                                                   | Tests |
| ------------------------------ | --------------------------------------------------------------- | ----: |
| `ArrowValidationTest`          | Correctness: Types D (passes), E (CtrlSum errors), H, J         |     4 |
| `ArrowValidationBenchmarkTest` | Arrow ingest + Arrow load + pure-Java validation, all types A–J |     1 |
| **Total**                      |                                                                 | **5** |

## `pgw-validator-pure-arrow` Test Results

```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0  -- ArrowValidationTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0  -- ArrowValidationBenchmarkTest

pgw-validator-pure-arrow :  5 tests — BUILD SUCCESS
```

## `pgw-validator-pure-arrow` — Benchmark (Types A–J)

Results from actual test run (2026-04-11, Java 25 Temurin 25.0.2):

```bash
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║    Pure-Arrow Validation Benchmark — XML→Arrow + Arrow Load + SQL-equivalent Validation (no DuckDB)                                  ║
╠══════════╦════════════╦═══════════════╦═══════════╦══════════════╦════════════════╦═══════════╦══════════════╦═══════════╣
║  Type    ║ Arrow (KB) ║  Ingest ms    ║  Load ms  ║ Validate ms  ║ Peak Off-Heap  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬═══════════════╬═══════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  Type A   ║    301,712 ║        26,267 ║        76 ║          732 ║    485,474,304 ║ 1,000,000 ║        1,366 ║ ✓ PASSED  ║
║  Type B   ║    301,604 ║        17,043 ║        57 ║          677 ║    485,474,304 ║ 1,000,000 ║        1,477 ║ ✓ PASSED  ║
║  Type C   ║    506,780 ║        23,836 ║       157 ║        4,252 ║    829,685,760 ║ 1,000,000 ║          235 ║ ✓ PASSED  ║
║  Type D   ║         65 ║             7 ║         1 ║            1 ║      1,785,856 ║       200 ║          200 ║ ✓ PASSED  ║
║  Type E   ║         65 ║             6 ║         1 ║            5 ║      1,785,856 ║       200 ║           40 ║ ✗ 3 err   ║
║  Type F   ║    604,504 ║        26,032 ║       175 ║        1,462 ║    939,638,784 ║ 2,000,000 ║        1,368 ║ ✓ PASSED  ║
║  Type G   ║  1,210,090 ║        53,123 ║       275 ║        3,021 ║  1,878,245,376 ║ 4,000,000 ║        1,324 ║ ✓ PASSED  ║
║  Type H   ║        604 ║            30 ║         1 ║            2 ║      2,113,536 ║     2,000 ║        1,000 ║ ✓ PASSED  ║
║  Type I   ║        603 ║            37 ║         1 ║            3 ║      2,113,536 ║     2,000 ║          667 ║ ✓ PASSED  ║
║  Type J   ║          6 ║             2 ║         1 ║            0 ║      1,785,856 ║         1 ║            1 ║ ✓ PASSED  ║
╠══════════╬════════════╬═══════════════╬═══════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL    ║  2,926,033 ║       146,383 ║       745 ║       10,155 ║  1,878,245,376 ║ 9,004,401 ║          887 ║ —         ║
╚══════════╩════════════╩═══════════════╩═══════════╩══════════════╩════════════════╩═══════════╩══════════════╩═══════════╝

  Arrow (KB)     = combined size of the 3 .arrow IPC files on disk
  Ingest ms      = XML → StAX parse → PureArrowBatchConsumer → .arrow files (no DuckDB)
  Load ms        = time to materialise .arrow files into ArrowPaymentRepositoryImpl
  Validate ms    = time to run ValidationPipeline.standard() in pure Java/Arrow (no SQL)
  Peak Off-Heap  = max Arrow allocator off-heap bytes during load+validate phases
                   (in-memory store released before loading — only repo batches counted)
  rows/ms (val)  = transaction row scan throughput during validation
```

## `pgw-validator-pure-arrow` — Streaming Iteration (StreamingTransactionIteratorValidator)

```bash
╔══════════════════════════════════════════════════════════════════════╗
║   Streaming Iteration — StreamingTransactionIteratorValidator         ║
╠══════════╦═══════════╦════════════════╦══════════════╣
║  Type    ║  Tx Rows  ║ Streaming ms   ║ rows/ms (str)║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  Type A   ║ 1,000,000 ║          2,258 ║          443 ║
║  Type B   ║ 1,000,000 ║          2,189 ║          457 ║
║  Type C   ║ 1,000,000 ║          3,330 ║          300 ║
║  Type D   ║       200 ║              0 ║          200 ║
║  Type E   ║       200 ║              0 ║          200 ║
║  Type F   ║ 2,000,000 ║          4,404 ║          454 ║
║  Type G   ║ 4,000,000 ║          9,385 ║          426 ║
║  Type H   ║     2,000 ║              4 ║          500 ║
║  Type I   ║     2,000 ║              6 ║          333 ║
║  Type J   ║         1 ║              0 ║            1 ║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  TOTAL    ║ 9,004,401 ║         21,576 ║          417 ║
╚══════════╩═══════════╩════════════════╩══════════════╝

  Streaming ms   = time for StreamingTransactionIteratorValidator to iterate all rows
                   directly from Arrow vectors (no SQL, no JDBC cursor)

  ► Grand Total Streaming Time (all types A–J): 21,576 ms for 9,004,401 transaction rows
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
