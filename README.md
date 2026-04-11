# PGW — ISO 20022 pain.001 → Apache Arrow + DuckDB Loader

📊 See [Ingestion Benchmark Results](INGESTOR_BENCHMARK_RESULTS.md) and [Validator Benchmark Results](VALIDATOR_BENCHMARK_RESULTS.md) for detailed performance data, test outcomes, and the DuckDB vs Pure-Arrow pipeline comparisons.

A study project that parses ISO 20022 **pain.001.001.09** (CustomerCreditTransferInitiation) XML files
into **Apache Arrow** columnar in-memory tables using a streaming StAX parser — with no DOM, no JAXB,
and no intermediate POJOs — then validates the data through a chainable SQL-based validation pipeline.

The goal is to measure whether Apache Arrow's columnar format provides meaningful gains in **storage**,
**memory**, and **analytical throughput** over raw XML for financial messaging workloads, and to compare
two Arrow ingest pipelines: one backed by DuckDB (SQL engine) and one that writes Arrow IPC files
directly without any intermediate SQL layer.

---

## Tech Stack

| Component    | Version                                               |
|--------------|-------------------------------------------------------|
| Java         | 25 (virtual threads via `Executors.newVirtualThreadPerTaskExecutor`) |
| Apache Arrow | 15.0.2 (`arrow-vector`, `arrow-memory-unsafe`, `arrow-c-data`) |
| DuckDB       | 1.4.4.0 (`duckdb_jdbc`) — in-process SQL engine       |
| XML Parser   | StAX (`javax.xml.stream`) — streaming pull parser     |
| Build        | Maven 3.9+, multi-module                              |
| Logging      | SLF4J 2.0.12                                          |

> **No DuckDB extensions required.** Arrow export and import use DuckDB's built-in
> C Data Interface (`DuckDBResultSet.arrowExportStream` / `registerArrowStream`).
> Works fully in air-gapped environments without `INSTALL arrow` or network access.

---

## Module Structure

```
pain001-arrow-loader/               ← parent POM (com.pgw:pain001-arrow-loader)
├── pgw-domain/                     ← pure-Java domain layer (VOs, exceptions, models)
├── pgw-common/                     ← shared infrastructure (Arrow schema, parser, generator,
│                                   │   benchmark, PaymentRepository interface, ValidationPipeline)
├── pgw-duckdb-helper/              ← shared DuckDB + Arrow C-Data utilities (ArrowIpc, DuckDbFactory,
│                                   │   StreamingBatchConsumer) — no domain
├── pgw-ingestor/                   ← XML → DuckDB → Arrow IPC pipeline (depends on pgw-duckdb-helper)
├── pgw-ingestor-pure-arrow/        ← XML → Arrow IPC only, no DuckDB (depends on pgw-common)
├── pgw-validator/                  ← DuckDB-backed PaymentRepositoryImpl + App (depends on pgw-duckdb-helper)
└── pgw-validator-pure-arrow/       ← pure-Arrow validation, no DuckDB (depends on pgw-common + pgw-domain)
```

**Dependency graph:**
```
pgw-domain
    ↑
pgw-common  ←─────────────────────────┐
    ↑                                 │
pgw-duckdb-helper               pgw-ingestor-pure-arrow
    ↑              ↑
pgw-ingestor    pgw-validator         pgw-validator-pure-arrow
```

### `pgw-common` — Shared Infrastructure + Validation Stack

Holds all infrastructure shared by more than one module: Arrow schema, StAX parser, XML generator,
benchmark utility, the `PaymentRepository` interface, the chainable `ValidationPipeline`, all
SQL-based validators, and the pure-Java domain service validators. Depends on `pgw-domain`.

```
pgw-common/src/main/java/com/pgw/
├── arrow/Pain001ArrowSchema.java         # Arrow schema definitions for all 3 tables
├── benchmark/LoadBenchmark.java          # Timing, memory tracking, formatted report
├── dal/PaymentRepository.java            # Interface: streaming access to message/remittance/transaction
├── domain/service/                       # Pure-Java domain validators (Iban, Bic, Amount, ControlSum)
│   ├── MessageDomainValidator.java
│   ├── RemittanceDomainValidator.java
│   ├── TransactionDomainValidator.java
│   └── ControlSumDomainService.java
├── generator/
│   ├── PainFileSpec.java                 # Data record: file spec (name, counts, flags)
│   ├── PainXmlGenerator.java             # Interface: generate(PainFileSpec, Path)
│   └── PainXmlGeneratorImpl.java         # StAX XML generator implementation
├── parser/
│   ├── BatchConsumer.java                # @FunctionalInterface: per-batch callback
│   ├── PainParser.java                   # Interface: parseStreaming()
│   ├── PainParserImpl.java               # StAX impl — streaming path clears RAM per batch
│   └── ParseStats.java                   # Lightweight result: (msgRows, rmtRows, txRows)
└── validation/
    ├── Validator.java                    # Interface: validate(PaymentRepository, ValidationContext)
    ├── ValidationContext.java            # Thread-safe error/warning collection
    ├── ValidationPipeline.java           # Fluent builder with virtual thread support
    ├── ExecutionMode.java                # SEQUENTIAL, PARALLEL, AUTO
    ├── ChainedValidator.java             # andThen() implementation
    ├── VirtualThreadValidator.java       # Abstract base for virtual thread execution
    └── validators/                       # Validators (SQL via PaymentRepository + domain service)
        ├── MessageValidator.java
        ├── RemittanceValidator.java
        ├── TransactionValidator.java
        ├── ControlSumValidator.java
        ├── ParallelTransactionValidator.java
        └── StreamingTransactionIteratorValidator.java
```

### `pgw-domain` — Pure-Java Domain Layer

Holds value objects, domain exceptions, and domain models. No Arrow or DuckDB dependency.

```
pgw-domain/src/main/java/com/pgw/
└── domain/
    ├── exception/                  # Typed validation exceptions (IBAN, BIC, Amount, Currency, ControlSum)
    ├── model/                      # Read-model records (Message, Remittance, Transaction, PaymentMethod)
    └── valueobject/                # VOs with enforced invariants (Iban, Bic, Amount, Currency, ControlSum)
```

### `pgw-duckdb-helper` — Shared DuckDB + Arrow Utilities

Holds all shared DuckDB/Arrow glue code. Both `pgw-ingestor` and `pgw-validator` depend on this
module. No domain dependency. Eliminates the previous `pgw-validator` → `pgw-ingestor` coupling.

```
pgw-duckdb-helper/src/main/java/com/pgw/
├── ArrowIpc.java                   # Extension-less Arrow export + load (C Data Interface)
├── DuckDbFactory.java              # Opens a plain DuckDB connection
└── parser/
    └── StreamingBatchConsumer.java # Single DuckDB sink: Arrow C Data Interface INSERT
```

### `pgw-ingestor` — DuckDB Ingest Pipeline

Owns the DuckDB ingest concern: StAX XML parsing → Arrow batch consumer → DuckDB live INSERT →
`ArrowIpc.export()` via C Data Interface. No domain objects. Depends on `pgw-common` and
`pgw-duckdb-helper`. Has no main-source Java files of its own — all infrastructure classes live
in `pgw-duckdb-helper`.

### `pgw-ingestor-pure-arrow` — Pure-Arrow Ingest Pipeline (no DuckDB)

Owns a DuckDB-free ingest path: StAX XML parsing → `PureArrowBatchConsumer` →
`ArrowStreamWriter` → `.arrow` files. Depends on `pgw-common`. Same parser, same schema,
different consumer — no SQL engine involved.

```
pgw-ingestor-pure-arrow/src/main/java/com/pgw/purearrow/
├── PureArrowBatchConsumer.java     # BatchConsumer: Arrow TransferPair → store + ArrowStreamWriter
├── PureArrowInMemoryStore.java     # Holds ArrowRecordBatch lists per table; AutoCloseable
├── PureArrowIngestResult.java      # Record: ParseStats + store + 3 file paths
└── PureArrowIngestor.java          # Orchestrates parse → consumer → result
```

### `pgw-validator` — DuckDB-Backed Repository + Application

Provides the DuckDB implementation of `PaymentRepository` (`PaymentRepositoryImpl`) and the
`App` entry point. Domain model, validation pipeline, and validators all live in `pgw-common`.
Depends on `pgw-common`, `pgw-domain`, and `pgw-duckdb-helper`.

```
pgw-validator/src/main/java/com/pgw/
├── App.java                              # Entry point — requires an existing pain.001 XML file path
└── dal/
    └── PaymentRepositoryImpl.java        # DuckDB implementation (zero-copy Arrow C Data Interface)
```

### `pgw-validator-pure-arrow` — Pure-Arrow Validation (no DuckDB)

Implements the **same `PaymentRepository` interface** as `pgw-validator`, but reads data directly
from Arrow IPC stream files via `ArrowStreamReader` — no DuckDB, no JDBC, no SQL. All validation
logic (IBAN regex, amount checks, control sum aggregation) is pure Java scanning Arrow vectors.
Depends on `pgw-common` and `pgw-domain`.

**Modular DAL design** (each class has a single clear responsibility):

```
pgw-validator-pure-arrow/src/main/java/com/pgw/purearrow/validator/dal/
├── ArrowTableLoader.java           # Utility: reads Arrow IPC stream file → List<VectorSchemaRoot>
│                                   #   Uses FieldVector.makeTransferPair(dst).transfer()
│                                   #   for zero-copy buffer ownership transfer (no duplicate)
├── ArrowMessageTable.java          # message table: typed column access + row iteration
├── ArrowRemittanceTable.java       # remittance table: typed column access + row iteration
│                                   #   (supports forEachByMsgId for filtered streaming)
├── ArrowTransactionTable.java      # transactions table: typed column access + row iteration
│                                   #   (supports forEachByPmtInfId for filtered streaming)
├── ArrowPaymentRepositoryImpl.java # Implements PaymentRepository using the 3 table classes
└── ArrowPaymentRepositoryLoader.java # Factory: load(msgFile, rmtFile, txFile, allocator)
```

| Test class | Module | What it measures |
|------------|--------|-----------------|
| `ParsePipelineTest` | `pgw-ingestor` | StAX parser correctness |
| `StreamingPipelineTest` | `pgw-ingestor` | Streaming memory footprint, DuckDB row counts, ArrowIpc export/load |
| `MemoryLeakVerificationTest` | `pgw-ingestor` | 50-iteration streaming parse: 0 bytes leaked |
| `IngestionBenchmarkTest` | `pgw-ingestor` | XML → DuckDB → Arrow IPC benchmark (Types A–J) |
| `PureArrowParsePipelineTest` | `pgw-ingestor-pure-arrow` | Pure-Arrow parser correctness (Types D + E) |
| `PureArrowStreamingPipelineTest` | `pgw-ingestor-pure-arrow` | Memory footprint, row counts, IPC round-trip |
| `PureArrowMemoryLeakVerificationTest` | `pgw-ingestor-pure-arrow` | 50-iteration zero-leak test (Types D + E) |
| `PureArrowIngestionBenchmarkTest` | `pgw-ingestor-pure-arrow` | XML → Arrow IPC benchmark, no DuckDB (Types A–J) |
| `ValidationTest` | `pgw-validator` | Domain validation correctness (DuckDB-backed) |
| `ArrowFileLoadBenchmarkTest` | `pgw-validator` | Arrow file → DuckDB load time (no validation) |
| `ValidationBenchmarkTest` | `pgw-validator` | Arrow→DuckDB load time **+ SQL validation time** |
| `ArrowValidationTest` | `pgw-validator-pure-arrow` | Correctness: Types D, E, H, J (pure-Arrow, no DuckDB) |
| `ArrowValidationBenchmarkTest` | `pgw-validator-pure-arrow` | Arrow ingest + Arrow load + Java validation benchmark (Types A–J) |

---

## Pipeline Architecture

### Pipeline A — DuckDB Ingest Pipeline (`pgw-ingestor`)

```
┌─────────────┐    StAX      ┌──────────────────────────────────────────────┐
│  pain.001   │──streaming──▶│  PainParserImpl.parseStreaming()              │
│  XML file   │   parse      │  (one VectorSchemaRoot per table, reused)     │
└─────────────┘              └──────────────┬───────────────────────────────┘
                                             │ BatchConsumer.accept() per 65k rows
                                             ▼
                              ┌──────────────────────────────────────────────┐
                              │  StreamingBatchConsumer                      │
                              │  └─ Single Sink: DuckDB (C Data Interface)   │
                              └──────────────────────────────────────────────┘
                                                   │
                                                   ▼
                                         ┌─────────────────┐
                                         │  DuckDB         │
                                         │  (live tables)  │
                                         └────────┬────────┘
                                                  │
                                    ┌─────────────┴──────────────────────┐
                                    │  ArrowIpc.export()                 │
                                    │  DuckDBResultSet.arrowExportStream │
                                    │  (C Data Interface — no extension) │
                                    └─────────────┬──────────────────────┘
                                                  │
                              ┌───────────────────┼───────────────────┐
                              ▼                   ▼                   ▼
                    _message.arrow      _remittance.arrow   _transaction.arrow
                              └───────────────────┴───────────────────┘
                                                  │
                                    ArrowIpc.load() → registerArrowStream
                                                  │
                                        PaymentRepository
                                                  │
                                ┌─────────────────┴──────────────────┐
                                │     ValidationPipeline              │
                                │  ├─ MessageValidator      (║)       │
                                │  ├─ RemittanceValidator   (║)       │  parallel virtual threads
                                │  ├─ TransactionValidator  (║)       │
                                │  │  (SQL via PaymentRepository)     │
                                │  ├─ MessageDomainValidator (║)      │
                                │  ├─ RemittanceDomainValidator(║)    │
                                │  ├─ TransactionDomainValidator      │  (domain VOs: Iban, Bic, Amount, …)
                                │  └─ ControlSumDomainService ──▶     │  sequential
                                └────────────────────────────────────┘
```

### Pipeline B — Pure-Arrow Ingest Pipeline (`pgw-ingestor-pure-arrow`)

```
┌─────────────┐    StAX      ┌──────────────────────────────────────────────┐
│  pain.001   │──streaming──▶│  PainParserImpl.parseStreaming()              │
│  XML file   │   parse      │  (same parser as Pipeline A — from pgw-common)│
└─────────────┘              └──────────────┬───────────────────────────────┘
                                             │ BatchConsumer.accept() per 65k rows
                                             ▼
                              ┌──────────────────────────────────────────────┐
                              │  PureArrowBatchConsumer                      │
                              │  └─ VectorUnloader → PureArrowInMemoryStore  │
                              │  └─ ArrowStreamWriter per table (lazy open)  │
                              └──────────────────────────────────────────────┘
                                             │
                              ┌──────────────┴──────────────┐
                              ▼                              ▼
                    PureArrowInMemoryStore              .arrow files
                    (ArrowRecordBatch lists)            (Arrow IPC stream,
                     — released by close()              same format as Pipeline A)
```

**Key difference vs Pipeline A:** No DuckDB is involved during ingest. The XML is parsed
directly into Arrow IPC stream files via `ArrowStreamWriter`. This eliminates the DuckDB
INSERT round-trip and Arrow→DuckDB→Arrow overhead, making ingest significantly faster for
scenarios where downstream DuckDB access is not needed at ingest time.

See [INGESTOR_BENCHMARK_RESULTS.md](INGESTOR_BENCHMARK_RESULTS.md#duckdb-vs-pure-arrow-pipeline-comparison) for a
side-by-side performance comparison of both pipelines across Types A–J.


### Arrow Export/Load — Extension-less C Data Interface

```
Export (DuckDB → .arrow file):
  DuckDB table
      ↓  DuckDBResultSet.arrowExportStream(allocator, 65536)
         DuckDB-native: native_stream → ArrowArrayStream → ArrowReader
  ArrowReader  (one batch at a time — no full-table accumulation)
      ↓  ArrowStreamWriter
  .arrow file (Arrow IPC stream format)

Load (.arrow file → DuckDB):
  .arrow file → ArrowStreamReader
      ↓  Data.exportArrayStream → ArrowArrayStream
  DuckDBConnection.registerArrowStream(tmpName, stream)
      ↓  CREATE TABLE <name> AS SELECT * FROM <tmpName>
  DuckDB table  (one scan pass — DuckDB calls get_next() batch by batch)
```

The XML is parsed in a **single streaming pass** directly into three relational Arrow tables:

| Table           | Source Element                              | Key                              | Description |
|-----------------|---------------------------------------------|----------------------------------|-------------|
| **Message**     | `GrpHdr` (GroupHeader85)                    | `msg_id` (PK)                    | One row per file — message ID, creation timestamp, total count, control sum |
| **Remittance**  | `PmtInf` (PaymentInstruction30)             | `pmt_inf_id` (PK), `msg_id` (FK) | One row per payment block — debtor info, control sum, execution date |
| **Transaction** | `CdtTrfTxInf` (CreditTransferTransaction34) | `pmt_inf_id` (FK)                | One row per credit transfer — amount, currency, creditor info |

Arrow type mappings follow ISO 20022 data type definitions:
- Text fields (`Max35Text`, `Max140Text`, IBAN, BIC) → `Utf8`
- Control sums (`DecimalNumber`) → `Decimal128(18, 2)`
- Transaction amounts → `Decimal128(18, 5)`
- Dates (`ISODate`) → `Date(DAY)`

---

## Configuration

| Environment Variable    | Default                                  | Description |
|-------------------------|------------------------------------------|-------------|
| `PAIN_LOCAL_OUTPUT_DIR` | `src/main/resources/output`              | Output directory for Arrow files |

---

## Running

```bash
# Prerequisites: Java 25, Maven 3.9+
# No DuckDB extensions needed — Arrow export/load uses the C Data Interface.

# Build all modules
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn clean package -DskipTests

# ── Run the application (no benchmark) ────────────────────────────────────────

# Writes .arrow files to src/main/resources/output/
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn exec:java -pl pgw-validator -Dexec.args="path/to/pain001.xml"

# Custom output directory
PAIN_LOCAL_OUTPUT_DIR=/data/arrows \
  MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn exec:java -pl pgw-validator -Dexec.args="path/to/pain001.xml"

# ── Run tests and see benchmark table output in the console ───────────────────
# Both module POMs set redirectTestOutputToFile=false so System.out from
# JUnit tests (the benchmark tables) is printed directly to the console.

# Fast tests only — no large file generation (Types D + E, ~200 rows each, < 5 s)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor \
  -Dtest="StreamingPipelineTest,ParsePipelineTest"

# Pure-Arrow fast tests (Types D + E only, < 5 s)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor-pure-arrow \
  -Dtest="PureArrowParsePipelineTest,PureArrowStreamingPipelineTest"

# Pure-Arrow ingest benchmark (Types A–J, no DuckDB)
# First run generates large files ~10 min; subsequent runs use cached files
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor-pure-arrow -Dtest=PureArrowIngestionBenchmarkTest

# Validation-stage benchmark only (Arrow → DuckDB load + SQL validation)
# Generates Types A–E Arrow files if absent; runs quickly once files exist
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-validator -Dtest=ValidationBenchmarkTest

# Arrow → DuckDB load benchmark only (no validation)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-validator -Dtest=ArrowFileLoadBenchmarkTest

# ── Run the full test suite (all benchmarks) ──────────────────────────────────

MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test

# Standardized before/after comparison script (saves timestamped log)
./run_validation_tests.sh
```

### How to read the benchmark tables

When tests run, benchmark output is printed directly to the console (not redirected to a file):

**ArrowFileLoadBenchmarkTest** — Arrow file → DuckDB load (downstream simulation):
```
╔══════════╦═══════════╦═══════════╦════════════╦═══════════╦════════════╦══════════════╦═════════════╗
║  Type    ║  Msg KB   ║  Rmt KB   ║  Tx KB     ║ Total KB  ║ Load (ms)  ║  Rows/sec    ║  Tx Rows    ║
╠══════════╬═══════════╬═══════════╬════════════╬═══════════╬════════════╬══════════════╬═════════════╣
║  Type A  ║         0 ║         2 ║    301,709 ║   301,712 ║        598 ║    1,672,242 ║   1,000,000 ║
...
```

**ValidationBenchmarkTest** — DuckDB load time + SQL validation time separated:
```
╔══════════╦════════════╦══════════════╦══════════════╦═══════════╦══════════════╦═══════════╗
║  Type    ║ Arrow (KB) ║ DuckDB ms    ║ Validate ms  ║  Tx Rows  ║ rows/ms (val)║  Result   ║
╠══════════╬════════════╬══════════════╬══════════════╬═══════════╬══════════════╬═══════════╣
║  Type A  ║    301,712 ║          602 ║           74 ║ 1,000,000 ║       13,514 ║ ✓ PASSED  ║
...
║  TOTAL   ║  1,110,226 ║        2,378 ║          497 ║ 3,000,400 ║        6,037 ║ —         ║
```

---

## Test Files

| Type | File | Structure | Remittances | Txns/Block | Total Txns | Purpose |
|------|------|-----------|-------------|------------|------------|---------|
| A | `pain001_type_a_1x1M.xml` | Fat batch | 1 | 1,000,000 | 1,000,000 | Benchmark |
| B | `pain001_type_b_2x500K.xml` | Two batches | 2 | 500,000 | 1,000,000 | Benchmark |
| C | `pain001_type_c_1Mx1.xml` | Many small | 1,000,000 | 1 | 1,000,000 | Benchmark |
| D | `pain001_type_d_2x100_valid.xml` | Small valid | 2 | 100 | 200 | Unit test |
| E | `pain001_type_e_2x100_invalid_ctrlsum.xml` | Small invalid | 2 | 100 | 200 | Negative test |
| F | `pain001_type_f_1x2M.xml` | Very fat batch | 1 | 2,000,000 | 2,000,000 | Large-scale benchmark |
| G | `pain001_type_g_1x4M.xml` | Extreme batch | 1 | 4,000,000 | 4,000,000 | Extreme-scale benchmark |
| H | `pain001_type_h_10x200.xml` | Multi-remittance | 10 | 200 | 2,000 | Multi-remittance correctness |
| I | `pain001_type_i_5x400.xml` | Multi-remittance | 5 | 400 | 2,000 | Multi-remittance variant |
| J | `pain001_type_j_1x1.xml` | Unitary | 1 | 1 | 1 | Unitary baseline |

> Types A–C, F, G are large files (~516 MB – 2.9 GB) not committed to the repo; generated on first test run.
> Types D–E, H–J are small (< 2 MB) and generated automatically by `mvn test`.

---

## Memory: Streaming vs Batch Accumulation

| Metric | Before (batch accumulation) | After (streaming) |
|--------|-----------------------------|-------------------|
| Arrow RAM during parse | O(file_size) — all batches live | O(1 batch) ≈ ~24 MB |
| Peak pod footprint | Heap + all Arrow batches + DuckDB | Heap + 1 batch + DuckDB |
| 10 M row file | ~8 GB Arrow off-heap | ~150 MB Arrow off-heap |
| Pod limit (4 GB) | OOM for large files | Fits comfortably |

Arrow export via `ArrowIpc.export` processes **one batch (65,536 rows)** at a time —
no full-table accumulation in either export or load direction.

---

## Validation Framework

```
ValidationPipeline
├─ MessageValidator            (parallel)  ─┐
├─ RemittanceValidator         (parallel)  ─┤  SQL via PaymentRepository
├─ TransactionValidator        (parallel)  ─┘
├─ MessageDomainValidator      (parallel)  ─┐
├─ RemittanceDomainValidator   (parallel)  ─┤  Pure-Java domain VOs
├─ TransactionDomainValidator  (sequential) ┘  (Iban, Bic, Amount, Currency)
└─ ControlSumDomainService     (sequential) ── streaming arithmetic check
```

### SQL Validators (via `PaymentRepository`)

| Validator | Checks |
|-----------|--------|
| `MessageValidator` | `MsgId` ≤ 35 chars; `InitgPty` non-blank; `CreDtTm` present |
| `RemittanceValidator` | IBAN regex `^[A-Z]{2}[0-9]{2}[A-Z0-9]+$`; payment method required |
| `TransactionValidator` | Amount > 0; creditor name non-blank |
| `ControlSumValidator` | Remittance + message level JOIN-based control sum comparison |

### Domain Validators (pure Java via domain VOs)

| Service | Checks |
|---------|--------|
| `MessageDomainValidator` | `messageId` ≤ 35; `initiatingParty` non-blank; `creationDateTime` ISO 8601 offset |
| `RemittanceDomainValidator` | debtor `Iban` MOD-97; debtor `Bic` SWIFT regex; `requestedExecutionDate` non-null |
| `TransactionDomainValidator` | `Amount(BigDecimal, Currency)` VOs; creditor `Bic`; creditor `Iban`; creditor name non-blank |
| `ControlSumDomainService` | streaming arithmetic: per-remittance and message-level `ControlSum.matches()` |

---

## Arrow File Sharing

```
Producer App                          Consumer App A
  parse XML once (streaming)          load .arrow files into DuckDB:  ~580–1,180 ms
  ArrowIpc.export ──disk/S3──▶        run SQL analytics
  3 × .arrow files
                                      (1M rows, no DuckDB extension needed)

                  ──disk/S3──▶         Consumer App B: same ~580 ms
                  ──disk/S3──▶         Consumer App C: same ~580 ms
```

---

## Performance Results

See [INGESTOR_BENCHMARK_RESULTS.md](INGESTOR_BENCHMARK_RESULTS.md) for ingestion benchmark results including:
- **DuckDB pipeline benchmarks** — all 10 types A–J (parse+insert ms, export ms, peak off-heap)
- **Pure-Arrow pipeline benchmarks** — all 10 types A–J (parse ms, peak off-heap)
- **Ingest side-by-side comparison** — DuckDB total vs Pure-Arrow, with speedup ratio per type

See [VALIDATOR_BENCHMARK_RESULTS.md](VALIDATOR_BENCHMARK_RESULTS.md) for validation benchmark results including:
- Arrow IPC Stream → DuckDB downstream load times (via `ArrowIpc.load()` — no extension) — all 10 types A–J
- **DuckDB SQL validation benchmark** — DuckDB load ms + SQL validation ms separated (Types A–J)
- **Pure-Arrow validation benchmark** — Arrow load ms + Java validation ms separated (Types A–J)
- Memory leak verification: 50-iteration stress test, 0 bytes leaked

---

## License

Study/research project. Use at your own discretion.

