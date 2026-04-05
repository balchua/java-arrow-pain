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
| AWS SDK v2   | 2.25.23 (`s3`) — optional, for S3 persistence mode    |
| Build        | Maven 3.9+, multi-module                              |
| Logging      | SLF4J 2.0.12                                          |

---

## Module Structure

```
pain001-arrow-loader/           ← parent POM (com.pgw:pain001-arrow-loader)
├── pgw-ingestor/               ← pure XML → Arrow → DuckDB pipeline (no domain)
└── pgw-validator/              ← all domain (model + VOs + DAL) + validation + App
```

### `pgw-ingestor` — Ingestion Pipeline

Owns the ingestion concern only: StAX XML parsing, Apache Arrow schema definition,
DuckDB live-INSERT via Arrow C Data Interface, and Arrow IPC Stream persistence.
Contains **no domain objects** — its only output is Arrow IPC Stream files and a
populated DuckDB connection.

```
pgw-ingestor/src/main/java/com/pgw/
├── arrow/
│   ├── Pain001ArrowSchema.java       # Arrow schema definitions for all 3 tables
│   └── ArrowBatchResult.java         # Legacy batch holder (used by --legacy path only)
├── benchmark/
│   └── LoadBenchmark.java            # Timing, memory tracking, formatted report
├── generator/
│   └── PainFileSpec.java             # Data record: file spec (name, counts, invalidControlSum flag)
├── parser/
│   ├── PainParser.java               # Interface: parseStreaming()
│   ├── PainParserImpl.java           # StAX impl — streaming path clears RAM per batch
│   ├── BatchConsumer.java            # @FunctionalInterface: per-batch callback
│   ├── ParseStats.java               # Lightweight result: (msgRows, rmtRows, txRows)
│   └── StreamingBatchConsumer.java   # Dual-sink: DuckDB live INSERT + PersistenceService
└── persistence/
    ├── PersistenceService.java           # Interface: writeBatch() + finish()
    ├── LocalFilePersistenceService.java  # Streams .arrows to configurable local dir
    ├── S3PersistenceService.java         # Streams .arrows to S3 multipart upload
    └── PersistenceServiceFactory.java    # Creates service from env vars
```

### `pgw-validator` — Full Domain + DAL + Validation + Application

Owns **all domain concerns**: ingestion read-model DTOs, validation value objects and
exceptions, domain validators, the DuckDB-backed repository (DAL), the chainable SQL
validation pipeline, and the `App` entry point. Depends on `pgw-ingestor`.

```
pgw-validator/src/main/java/com/pgw/
├── App.java                              # Entry point — requires an existing pain.001 XML file path
├── dal/
│   ├── PaymentRepository.java        # Interface: streaming SQL access to message/remittance/transaction
│   └── PaymentRepositoryImpl.java    # DuckDB implementation (zero-copy Arrow C Data Interface)
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
    ├── Validator.java                # Interface: validate(PaymentRepository, ValidationContext)
    ├── ValidationContext.java        # Thread-safe error/warning collection
    ├── ValidationPipeline.java       # Fluent builder with virtual thread support
    ├── ExecutionMode.java            # SEQUENTIAL, PARALLEL, AUTO
    ├── ChainedValidator.java         # andThen() implementation
    ├── VirtualThreadValidator.java   # Abstract base for virtual thread execution
    └── validators/                   # SQL-based validators (via PaymentRepository)
        ├── MessageValidator.java
        ├── RemittanceValidator.java
        ├── TransactionValidator.java
        ├── ControlSumValidator.java
        └── ParallelTransactionValidator.java

pgw-validator/src/test/java/com/pgw/
├── SampleGenerationTest.java         # JUnit 5: Type D (valid) + Type E (invalid CtrlSum)
├── StreamingPipelineTest.java        # JUnit 5: streaming memory, row counts, .arrows files
├── ArrowFileLoadBenchmarkTest.java   # JUnit 5: Arrow IPC Stream → DuckDB load speed benchmark
├── MemoryLeakVerificationTest.java   # JUnit 5: 50-iteration leak verification
├── FullPipelineBenchmarkTest.java    # JUnit 5: Full streaming pipeline A–E benchmark
├── SampleGeneratorRunner.java        # Runnable main: generate by type (a–e)
└── generator/
    ├── PainXmlGenerator.java         # Interface: generate(PainFileSpec, Path) → Path
    ├── PainXmlGeneratorImpl.java     # StAX implementation (honours invalidControlSum)
    ├── TestPainFileSpecs.java        # Test-only spec constants A–E
    └── TestFileGenerator.java        # generate-if-absent + file-complete check
```

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
                              │  ├─ Sink A: DuckDB (C Data Interface INSERT) │
                              │  └─ Sink B: PersistenceService               │
                              │       ├─ LocalFilePersistenceService (local) │
                              │       └─ S3PersistenceService (S3 upload)    │
                              └──────────────────────────────────────────────┘
                                  │                         │
                                  ▼                         ▼
                         ┌─────────────────┐    ┌───────────────────────┐
                         │  DuckDB         │    │  .arrows files        │
                         │  (live tables)  │    │  (IPC Stream format)  │
                         └────────┬────────┘    └───────────────────────┘
                                  │
                        PaymentRepository
                                  │
                  ┌───────────────┴────────────────┐
                  │     ValidationPipeline          │
                  │  ├─ MessageValidator      (║)   │
                  │  ├─ RemittanceValidator   (║)   │  parallel virtual threads
                  │  ├─ TransactionValidator  (║)   │
                  │  │  (SQL via PaymentRepository) │
                  │  ├─ MessageDomainValidator (║)  │
                  │  ├─ RemittanceDomainValidator(║)│
                  │  ├─ TransactionDomainValidator  │  (domain VOs: Iban, Bic, Amount, …)
                  │  └─ ControlSumDomainService ──▶ │  sequential
                  └─────────────────────────────────┘
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

| Environment Variable    | Default                        | Description |
|-------------------------|--------------------------------|-------------|
| `PAIN_PERSISTENCE_MODE` | `local`                        | Output sink: `local` or `s3` |
| `PAIN_LOCAL_OUTPUT_DIR` | `pgw-ingestor/src/main/resources/output` | Local output directory |
| `PAIN_S3_BUCKET`        | _(required for s3 mode)_       | Target S3 bucket name |
| `PAIN_S3_KEY_PREFIX`    | `pain001`                      | S3 key prefix (folder) |

---

## Running

```bash
# Prerequisites: Java 25, Maven 3.9+

# Build all modules
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn clean package -DskipTests

# Run the application (from pgw-validator module — it owns the App entry point)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn exec:java -pl pgw-validator -Dexec.args="path/to/pain001.xml"

# S3 mode
PAIN_PERSISTENCE_MODE=s3 PAIN_S3_BUCKET=my-bucket PAIN_S3_KEY_PREFIX=pain001/2026/02 \
  MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn exec:java -pl pgw-validator -Dexec.args="path/to/pain001.xml"

# Run full test suite (generates sample files, runs all benchmarks)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-validator --also-make

# Run a specific test class
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-validator --also-make -Dtest=ArrowFileLoadBenchmarkTest

# Standardized before/after comparison script
./run_validation_tests.sh
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

## Arrow File Sharing via S3

```
Producer App                          Consumer App A
  parse XML once (streaming)          download .arrows files (~224–362 MB)
  export Arrow IPC  ──S3──▶           load into DuckDB:  ~376–687 ms
  3 × .arrows files                   run SQL analytics

                    ──S3──▶           Consumer App B: same ~400 ms
                    ──S3──▶           Consumer App C: same ~400 ms
```

---

## Performance Results

See [TEST_RESULTS.md](TEST_RESULTS.md) for the full benchmark report including:
- Full pipeline benchmarks (all 5 types A–E)
- Java heap delta and Arrow off-heap peak for every type
- Per-table Arrow file sizes
- Arrow IPC Stream → DuckDB downstream load times
- Memory leak verification: 50-iteration stress test, 0 bytes leaked
- Full test suite summary (all 15 tests passing)

---

## License

Study/research project. Use at your own discretion.
