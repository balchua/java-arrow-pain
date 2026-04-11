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
- **Load is faster** in pure Arrow: zero-copy via `TransferPair` is faster than Arrow C Data Interface round-trip through DuckDB.
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
