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
║  Type A   ║         0 ║         2 ║    301,709 ║   301,712 ║        740 ║    121,576,854 ║    1,351,352 ║   1,000,000 ║
║  Type B   ║         0 ║         2 ║    301,600 ║   301,604 ║        706 ║    121,542,390 ║    1,416,433 ║   1,000,000 ║
║  Type C   ║         0 ║   205,125 ║    301,654 ║   506,780 ║      1,350 ║    121,511,222 ║    1,481,481 ║   1,000,000 ║
║  Type D   ║         0 ║         2 ║         61 ║        65 ║         15 ║         70,486 ║       13,466 ║         200 ║
║  Type E   ║         0 ║         2 ║         61 ║        65 ║         15 ║         70,486 ║       13,466 ║         200 ║
║  Type F   ║         0 ║         2 ║    604,501 ║   604,504 ║      1,340 ║    121,904,566 ║    1,492,538 ║   2,000,000 ║
║  Type G   ║         0 ║         2 ║  1,210,086 ║ 1,210,090 ║      1,977 ║    121,970,078 ║    2,023,268 ║   4,000,000 ║
║  Type H   ║         0 ║         4 ║        598 ║       604 ║         18 ║      1,053,750 ║      111,666 ║       2,000 ║
║  Type I   ║         0 ║         3 ║        599 ║       603 ║         18 ║      1,053,750 ║      111,388 ║       2,000 ║
║  Type J   ║         0 ║         2 ║          2 ║         5 ║         14 ║          5,944 ║          142 ║           1 ║
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
║  Type A   ║    301,712 ║          869 ║           96 ║    121,576,862 ║ 1,000,000 ║       10,417 ║ ✓ PASSED  ║
║  Type B   ║    301,604 ║          874 ║           88 ║    121,566,686 ║ 1,000,000 ║       11,364 ║ ✓ PASSED  ║
║  Type C   ║    506,780 ║        1,580 ║          362 ║    121,511,222 ║ 1,000,000 ║        2,762 ║ ✓ PASSED  ║
║  Type D   ║         65 ║           16 ║            6 ║         70,486 ║       200 ║           33 ║ ✓ PASSED  ║
║  Type E   ║         65 ║           18 ║           10 ║         70,486 ║       200 ║           20 ║ ✗ 3 err   ║
║  Type F   ║    604,504 ║        1,596 ║          136 ║    121,970,102 ║ 2,000,000 ║       14,706 ║ ✓ PASSED  ║
║  Type G   ║  1,210,090 ║        2,325 ║          249 ║    121,970,070 ║ 4,000,000 ║       16,064 ║ ✓ PASSED  ║
║  Type H   ║        604 ║           16 ║            8 ║      1,053,750 ║     2,000 ║          250 ║ ✓ PASSED  ║
║  Type I   ║        603 ║           17 ║            7 ║      1,053,750 ║     2,000 ║          286 ║ ✓ PASSED  ║
║  Type J   ║          5 ║           13 ║            7 ║          5,944 ║         1 ║            0 ║ ✓ PASSED  ║
╠══════════╬════════════╬══════════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL    ║  2,926,032 ║        7,324 ║          969 ║    121,970,102 ║ 9,004,401 ║        9,292 ║ —         ║
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
║  Type A   ║ 1,000,000 ║          7,879 ║          127 ║
║  Type B   ║ 1,000,000 ║          7,743 ║          129 ║
║  Type C   ║ 1,000,000 ║          7,532 ║          133 ║
║  Type D   ║       200 ║              3 ║           67 ║
║  Type E   ║       200 ║              2 ║          100 ║
║  Type F   ║ 2,000,000 ║         15,106 ║          132 ║
║  Type G   ║ 4,000,000 ║         29,748 ║          134 ║
║  Type H   ║     2,000 ║             16 ║          125 ║
║  Type I   ║     2,000 ║             16 ║          125 ║
║  Type J   ║         1 ║              0 ║            1 ║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  TOTAL    ║ 9,004,401 ║         68,045 ║          132 ║
╚══════════╩═══════════╩════════════════╩══════════════╝

  Streaming ms   = time for StreamingTransactionIteratorValidator to iterate rows via JDBC cursor
  ► Grand Total: 68,045 ms for 9,004,401 rows  ≈ 132 rows/ms (DuckDB JDBC streaming)
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

Results from actual test run (2026-04-11, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║    Pure-Arrow Validation Benchmark — XML→Arrow + Arrow Load + SQL-equivalent Validation (no DuckDB)                                  ║
╠══════════╦════════════╦═══════════════╦═══════════╦══════════════╦════════════════╦═══════════╦══════════════╦═══════════╣
║  Type    ║ Arrow (KB) ║  Ingest ms    ║  Load ms  ║ Validate ms  ║ Peak Off-Heap  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬═══════════════╬═══════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  Type A   ║    301,712 ║        12,378 ║        61 ║        2,722 ║    485,474,304 ║ 1,000,000 ║          367 ║ ✓ PASSED  ║
║  Type B   ║    301,604 ║        10,486 ║        33 ║        2,665 ║    485,474,304 ║ 1,000,000 ║          375 ║ ✓ PASSED  ║
║  Type C   ║    506,780 ║        17,334 ║        69 ║        5,740 ║    829,685,760 ║ 1,000,000 ║          174 ║ ✓ PASSED  ║
║  Type D   ║         65 ║             7 ║         1 ║            1 ║      1,785,856 ║       200 ║          200 ║ ✓ PASSED  ║
║  Type E   ║         65 ║             5 ║         1 ║            4 ║      1,785,856 ║       200 ║           50 ║ ✗ 3 err   ║
║  Type F   ║    604,504 ║        19,440 ║        78 ║        5,364 ║    939,638,784 ║ 2,000,000 ║          373 ║ ✓ PASSED  ║
║  Type G   ║  1,210,090 ║        38,590 ║       184 ║       10,556 ║  1,878,245,376 ║ 4,000,000 ║          379 ║ ✓ PASSED  ║
║  Type H   ║        604 ║            21 ║         1 ║            5 ║      2,113,536 ║     2,000 ║          400 ║ ✓ PASSED  ║
║  Type I   ║        603 ║            21 ║         1 ║            5 ║      2,113,536 ║     2,000 ║          400 ║ ✓ PASSED  ║
║  Type J   ║          6 ║             1 ║         1 ║            0 ║      1,785,856 ║         1 ║            1 ║ ✓ PASSED  ║
╠══════════╬════════════╬═══════════════╬═══════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣
║  TOTAL    ║  2,926,033 ║        98,283 ║       430 ║       27,062 ║  1,878,245,376 ║ 9,004,401 ║          333 ║ —         ║
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
║  Type A   ║ 1,000,000 ║          1,277 ║          783 ║
║  Type B   ║ 1,000,000 ║          1,339 ║          747 ║
║  Type C   ║ 1,000,000 ║          1,395 ║          717 ║
║  Type D   ║       200 ║              0 ║          200 ║
║  Type E   ║       200 ║              0 ║          200 ║
║  Type F   ║ 2,000,000 ║          2,659 ║          752 ║
║  Type G   ║ 4,000,000 ║          5,288 ║          756 ║
║  Type H   ║     2,000 ║              3 ║          667 ║
║  Type I   ║     2,000 ║              2 ║        1,000 ║
║  Type J   ║         1 ║              0 ║            1 ║
╠══════════╬═══════════╬════════════════╬══════════════╣
║  TOTAL    ║ 9,004,401 ║         11,963 ║          753 ║
╚══════════╩═══════════╩════════════════╩══════════════╝

  Streaming ms   = time for StreamingTransactionIteratorValidator to iterate all rows
                   directly from Arrow vectors (no SQL, no JDBC cursor)
  ► Grand Total: 11,963 ms for 9,004,401 rows  ≈ 753 rows/ms pure Arrow vector iteration
```

## `pgw-validator-pure-arrow` — DuckDB SQL vs Pure-Arrow Comparison (ValidatorComparisonBenchmarkTest)

Results from actual test run (2026-04-11, Java 25 Temurin 25.0.2, `-Xmx8g`):

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║    Validator Pipeline Comparison — DuckDB SQL vs Pure-Arrow Java (same ValidationPipeline.standard())                                                    ║
╠══════════╦══════════════════════════════════════════════════════════════════╦══════════════════════════════════════════════════════════════════════╦════════╣
║          ║              DuckDB path (SQL)                                   ║            Pure-Arrow path (Java)                                     ║        ║
║  Type    ║ Ingest ms │ Load ms  │ Val ms   │ Peak Off-Heap  │  Result       ║ Ingest ms │ Load ms  │ Val ms   │ Peak Off-Heap  │  Result       ║Speedup ║
╠══════════╬══════════════════════════════════════════════════════════════════╬══════════════════════════════════════════════════════════════════════╬════════╣
║  Type A   ║    16,640 │      627 │       70 │    121,587,294 │ ✓ PASS        ║     8,290 │       32 │    2,595 │    485,474,304 │ ✓ PASS        ║  1.59x ║
║  Type B   ║    12,509 │      667 │       81 │    121,465,798 │ ✓ PASS        ║     9,607 │       30 │    2,658 │    485,474,304 │ ✓ PASS        ║  1.08x ║
║  Type C   ║    30,202 │    1,265 │      341 │    121,521,902 │ ✓ PASS        ║    15,718 │       99 │    5,606 │    829,685,760 │ ✓ PASS        ║  1.48x ║
║  Type D   ║        35 │       14 │        7 │      1,787,592 │ ✓ PASS        ║         3 │        0 │        1 │      1,785,856 │ ✓ PASS        ║ 14.00x ║
║  Type E   ║        23 │       13 │        8 │      1,787,592 │ ✗ 3 err       ║         4 │        1 │        1 │      1,785,856 │ ✗ 3 err       ║  7.33x ║
║  Type F   ║    28,907 │    1,250 │      123 │    121,904,534 │ ✓ PASS        ║    16,476 │      109 │    5,411 │    939,638,784 │ ✓ PASS        ║  1.38x ║
║  Type G   ║    55,782 │    2,109 │      251 │    121,970,094 │ ✓ PASS        ║    31,893 │      299 │    9,943 │  1,878,245,376 │ ✓ PASS        ║  1.38x ║
║  Type H   ║        50 │       15 │        7 │      2,113,536 │ ✓ PASS        ║        18 │        0 │        5 │      2,113,536 │ ✓ PASS        ║  3.13x ║
║  Type I   ║        46 │       14 │        6 │      2,113,536 │ ✓ PASS        ║        20 │        0 │        5 │      2,113,536 │ ✓ PASS        ║  2.64x ║
║  Type J   ║        20 │       13 │        6 │      1,787,592 │ ✓ PASS        ║         1 │        0 │        0 │      1,785,856 │ ✓ PASS        ║ 39.00x ║
╚══════════╩══════════════════════════════════════════════════════════════════╩══════════════════════════════════════════════════════════════════════╩════════╝

  ► Total DuckDB  path: 151,101 ms for 9,004,401 transaction rows
  ► Total Arrow   path: 108,825 ms for 9,004,401 transaction rows
  ► Overall speedup: 1.39x (Arrow vs DuckDB end-to-end)
```

**Key observations:**
- **Same `ValidationPipeline.standard()`** runs unchanged on both backends (polymorphism through `PaymentRepository` interface).
- **Validation results are identical**: both pipelines agree on every pass/fail decision (Type E: 3 CtrlSum errors each).
- **Ingest is faster** with pure Arrow (no DuckDB INSERT round-trip): 1.1–39× faster per type.
- **Validation is slower** in pure Arrow (iterating Arrow vectors in Java vs DuckDB's compiled SQL): ~30–50× slower for large types.
- **Load is faster** in pure Arrow: zero-copy via `TransferPair` is faster than Arrow C Data Interface round-trip through DuckDB.
- **Peak off-heap is now correctly measured** for load+validate only: the in-memory ingest store is explicitly released before loading, so peak reflects only the repository batches held in the Arrow allocator.
  - Pure Arrow: ~485 MB for 1M tx (Type A/B), ~1,878 MB for 4M tx (Type G)
  - DuckDB: ~122 MB for any size (bounded — data held in DuckDB's own memory, not Arrow allocator)
- **End-to-end speed**: pure Arrow is **1.39×** faster overall (ingest savings outweigh validation overhead).

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
  PipelineComparisonBenchmarkTest   :  1 test  — PASS  (DuckDB vs Pure Arrow A–J, 1.25–6.5× speedup)
  ─────────────────────────────────────────────
  Subtotal                          : 12 tests — BUILD SUCCESS

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
  ValidatorComparisonBenchmarkTest :  1 test  — PASS  (DuckDB SQL vs Arrow Java, 1.08–39× speedup)
  ─────────────────────────────────────────────
  Subtotal                         :  6 tests — BUILD SUCCESS

══════════════════════════════════════════════════════════════
  TOTAL                      : 36 tests — BUILD SUCCESS
══════════════════════════════════════════════════════════════
```

**All 36 tests pass. Benchmark data above is from actual test runs on 2026-04-11 (Java 25 Temurin 25.0.2, -Xmx8g).**
