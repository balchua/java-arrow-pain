# PGW — ISO 20022 pain.001 → Apache Arrow + DuckDB Loader

📊 See [Test Results & Benchmark Report](TEST_RESULTS.md) for detailed performance data and test outcomes.

A study project that parses ISO 20022 **pain.001.001.09** (CustomerCreditTransferInitiation) XML files
into **Apache Arrow** columnar in-memory tables using a streaming StAX parser — with no DOM, no JAXB,
and no intermediate POJOs — then validates the data through a chainable SQL-based validation pipeline.

The goal is to measure whether Apache Arrow's columnar format provides meaningful gains in **storage**,
**memory**, and **analytical throughput** over raw XML for financial messaging workloads.

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
pain001-arrow-loader/           ← parent POM (com.pgw:pain001-arrow-loader)
├── pgw-ingestor/               ← pure XML → Arrow → DuckDB pipeline (no domain)
└── pgw-validator/              ← all domain (model + VOs + DAL) + validation + App
```

### `pgw-ingestor` — Ingestion Pipeline

Owns the ingestion concern only: StAX XML parsing, Apache Arrow schema definition,
and DuckDB live-INSERT via Arrow C Data Interface. Contains **no domain objects** —
its only output is a populated DuckDB connection, which can export Arrow files via
`ArrowIpc.export()` (extension-less, one batch at a time).

```
pgw-ingestor/src/main/java/com/pgw/
├── ArrowIpc.java                         # Extension-less Arrow export + load (C Data Interface)
├── DuckDbFactory.java                    # Opens a plain DuckDB connection (no extension loading)
├── arrow/
│   └── Pain001ArrowSchema.java           # Arrow schema definitions for all 3 tables
├── benchmark/
│   └── LoadBenchmark.java                # Timing, memory tracking, formatted report
├── generator/
│   └── PainFileSpec.java                 # Data record: file spec (name, counts, invalidControlSum flag)
└── parser/
    ├── PainParser.java                   # Interface: parseStreaming()
    ├── PainParserImpl.java               # StAX impl — streaming path clears RAM per batch
    ├── BatchConsumer.java                # @FunctionalInterface: per-batch callback
    ├── ParseStats.java                   # Lightweight result: (msgRows, rmtRows, txRows)
    └── StreamingBatchConsumer.java       # Single DuckDB sink: Arrow C Data Interface INSERT
```

### `pgw-validator` — Full Domain + DAL + Validation + Application

Owns **all domain concerns**: ingestion read-model DTOs, validation value objects and
exceptions, domain validators, the DuckDB-backed repository (DAL), the chainable SQL
validation pipeline, and the `App` entry point. Depends on `pgw-ingestor`.

```
pgw-validator/src/main/java/com/pgw/
├── App.java                              # Entry point — requires an existing pain.001 XML file path
├── dal/
│   ├── PaymentRepository.java            # Interface: streaming SQL access to message/remittance/transaction
│   └── PaymentRepositoryImpl.java        # DuckDB implementation (zero-copy Arrow C Data Interface)
├── domain/
│   ├── model/                            # Read-model DTOs hydrated from DuckDB by the DAL
│   │   ├── Message.java                  # GroupHeader — messageId, creationDateTime, controlSum, initiatingParty
│   │   ├── Remittance.java               # PaymentInformation — remittanceId, debtor, executionDate, controlSum
│   │   ├── Transaction.java              # CreditTransferTransaction — amounts, currency, creditor info
│   │   └── PaymentMethod.java            # Enum: TRF, CHK
│   ├── exception/                        # Typed validation exceptions
│   │   ├── InvalidIbanException.java
│   │   ├── InvalidBicException.java
│   │   ├── InvalidAmountException.java
│   │   ├── InvalidCurrencyException.java
│   │   └── InvalidControlSumException.java
│   ├── valueobject/                      # VOs with enforced invariants
│   │   ├── Iban.java                     # MOD-97 checksum (ISO 13616-1)
│   │   ├── Bic.java                      # SWIFT regex ^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$
│   │   ├── Amount.java                   # value > 0; carries Currency; add() guards same-currency
│   │   ├── Currency.java                 # ISO 4217 [A-Z]{3}
│   │   └── ControlSum.java               # value ≥ 0; scaled to 2dp; epsilon-safe matches()
│   └── service/                          # Pure-Java domain validators
│       ├── MessageDomainValidator.java   # messageId ≤ 35, initiatingParty non-blank, CreDtTm ISO 8601
│       ├── RemittanceDomainValidator.java # debtor Iban MOD-97, debtor Bic SWIFT, executionDate non-null
│       ├── TransactionDomainValidator.java # Amount+Currency VOs, creditor Iban+Bic, creditor name non-blank
│       └── ControlSumDomainService.java  # streaming arithmetic: rmt-level + msg-level ControlSum.matches()
└── validation/
    ├── Validator.java                    # Interface: validate(PaymentRepository, ValidationContext)
    ├── ValidationContext.java            # Thread-safe error/warning collection
    ├── ValidationPipeline.java           # Fluent builder with virtual thread support
    ├── ExecutionMode.java                # SEQUENTIAL, PARALLEL, AUTO
    ├── ChainedValidator.java             # andThen() implementation
    ├── VirtualThreadValidator.java       # Abstract base for virtual thread execution
    └── validators/                       # SQL-based validators (via PaymentRepository)
        ├── MessageValidator.java
        ├── RemittanceValidator.java
        ├── TransactionValidator.java
        ├── ControlSumValidator.java
        └── ParallelTransactionValidator.java
```

| Test class | Module | What it measures |
|------------|--------|-----------------|
| `SampleGenerationTest` | `pgw-ingestor` | XML sample generation (Types D + E) |
| `StreamingPipelineTest` | `pgw-ingestor` | Streaming memory footprint, DuckDB row counts, ArrowIpc export/load |
| `MemoryLeakVerificationTest` | `pgw-ingestor` | 50-iteration streaming parse: 0 bytes leaked |
| `ParsePipelineTest` | `pgw-ingestor` | StAX parser correctness |
| `ArrowFileLoadBenchmarkTest` | `pgw-validator` | Arrow file → DuckDB load time (no validation) |
| `ValidationBenchmarkTest` | `pgw-validator` | Arrow→DuckDB load time **+ SQL validation time** |
| `ValidationTest` | `pgw-validator` | Domain validation correctness |

---

## Pipeline Architecture

```
┌─────────────┐    StAX      ┌──────────────────────────────────────────────┐
│  pain.001   │──streaming──▶│  PainParserImpl.parseStreaming()              │
│  XML file   │   parse      │  (one VectorSchemaRoot per table, reused)     │
└─────────────┘               └──────────────┬───────────────────────────────┘
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
  -Dtest="SampleGenerationTest,StreamingPipelineTest,ParsePipelineTest"

# Validation-stage benchmark only (Arrow → DuckDB load + SQL validation)
# Generates Types A–E Arrow files if absent; runs quickly once files exist
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-validator -Dtest=ValidationBenchmarkTest

# Arrow → DuckDB load benchmark only (no validation)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-validator -Dtest=ArrowFileLoadBenchmarkTest

# ── Run the full test suite (all benchmarks) ──────────────────────────────────

MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor,pgw-validator

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

> Types A–C are large files (~516–888 MB) generated on demand. Types D–E are generated automatically by `mvn test`.

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

See [TEST_RESULTS.md](TEST_RESULTS.md) for the full benchmark report including:
- Full pipeline benchmarks (all 5 types A–E)
- Java heap delta and Arrow off-heap peak for every type
- Per-table Arrow file sizes
- Arrow IPC Stream → DuckDB downstream load times (via ArrowIpc — no extension)
- Memory leak verification: 50-iteration stress test, 0 bytes leaked
- Full test suite summary (15 tests passing)

---

## License

Study/research project. Use at your own discretion.

