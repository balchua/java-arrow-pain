# ISO 20022 pain.001 → Apache Arrow + DuckDB Loader

📊 See [Test Results & Benchmark Report](TEST_RESULTS.md) for detailed performance data and test outcomes.

A study project that parses ISO 20022 **pain.001.001.09** (CustomerCreditTransferInitiation) XML files into **Apache Arrow** columnar in-memory tables using a streaming StAX parser — with no DOM, no JAXB, and no intermediate POJOs.

The goal is to measure whether Apache Arrow's columnar format provides meaningful gains in **storage**, **memory**, and **analytical throughput** over raw XML for financial messaging workloads.

## Tech Stack

| Component    | Version                                           |
| ------------ | ------------------------------------------------- |
| Java         | 17+ (virtual thread support via reflection on 21+) |
| Apache Arrow | 15.0.2 (`arrow-vector`, `arrow-memory-unsafe`)    |
| DuckDB       | 1.1.3 (`duckdb_jdbc`) — in-process SQL engine     |
| XML Parser   | StAX (`javax.xml.stream`) — streaming pull parser |
| Build        | Maven 3.9.6                                       |
| Logging      | SLF4J 2.0.12                                      |

## Architecture

```
┌─────────────┐     StAX      ┌──────────────────┐     IPC      ┌─────────────┐
│  pain.001    │──streaming──▶│  Apache Arrow     │──zero-copy──▶│  .arrow     │
│  XML file    │   parse      │  off-heap vectors │   write      │  IPC files  │
└─────────────┘               └──────────────────┘              └─────────────┘
                                       │
                                       │ columnar scan
                                       ▼
                              ┌──────────────────┐
                              │  Validation      │
                              │  Pipeline        │
                              │  (4 validators)  │
                              └──────────────────┘
                                  │
                                  ├─ MessageValidator ──────┐
                                  ├─ RemittanceValidator ───┤ Parallel
                                  ├─ TransactionValidator ──┘ (virtual threads)
                                  │
                                  └─ ControlSumValidator ───── Sequential
```

The XML is parsed in a **single streaming pass** directly into three relational Arrow tables:

| Table           | Source Element                              | Key                              | Description                                                                    |
| --------------- | ------------------------------------------- | -------------------------------- | ------------------------------------------------------------------------------ |
| **Message**     | `GrpHdr` (GroupHeader85)                    | `msg_id` (PK)                    | One row per file — message ID, creation timestamp, total count, control sum    |
| **Remittance**  | `PmtInf` (PaymentInstruction30)             | `pmt_inf_id` (PK), `msg_id` (FK) | One row per payment block — debtor info, control sum, execution date           |
| **Transaction** | `CdtTrfTxInf` (CreditTransferTransaction34) | `pmt_inf_id` (FK)                | One row per credit transfer — amount, currency, creditor info, remittance info |

Arrow type mappings follow ISO 20022 data type definitions:
- Text fields (`Max35Text`, `Max140Text`, IBAN, BIC) → `Utf8`
- Control sums (`DecimalNumber`) → `Decimal128(18, 2)`
- Transaction amounts (`ActiveOrHistoricCurrencyAndAmount`) → `Decimal128(18, 5)`
- Dates (`ISODate`) → `Date(DAY)`

## Project Structure

```
src/main/java/com/iso20022/pain/
├── App.java                          # Entry point — requires an existing pain.001 XML file path
├── arrow/
│   ├── Pain001ArrowSchema.java       # Arrow schema definitions for all 3 tables
│   ├── ArrowBatchResult.java         # Holds VectorSchemaRoot tables (AutoCloseable)
│   └── ArrowFileExporter.java        # Writes Arrow IPC files (VectorUnloader/Loader pattern)
├── parser/
│   ├── PainParser.java               # Interface: parse(Path, BufferAllocator) → ArrowBatchResult
│   └── PainParserImpl.java           # Streaming StAX implementation (no POJOs)
├── generator/
│   └── PainFileSpec.java             # Data record: file spec (name, counts, invalidControlSum flag)
├── dal/
│   ├── PaymentRepository.java        # Interface: SQL access to message/remittance/transaction tables
│   └── PaymentRepositoryImpl.java    # DuckDB implementation (zero-copy Arrow C Data Interface)
├── validation/
│   ├── Validator.java                # Interface: validate(PaymentRepository, ValidationContext)
│   ├── ValidationContext.java        # Thread-safe error/warning collection
│   ├── ValidationPipeline.java       # Fluent builder with virtual thread support
│   ├── ExecutionMode.java            # SEQUENTIAL, PARALLEL, AUTO modes
│   ├── ChainedValidator.java         # andThen() implementation
│   ├── VirtualThreadValidator.java   # Abstract base for virtual thread execution
│   └── validators/
│       ├── MessageValidator.java     # SQL: MsgId length, InitgPty, CreDtTm
│       ├── RemittanceValidator.java  # SQL: IBAN format, payment method
│       ├── TransactionValidator.java # SQL: amounts > 0, creditor names
│       ├── ControlSumValidator.java  # SQL: JOIN-based control sum validation
│       └── ParallelTransactionValidator.java  # Batch-parallel transaction validator
└── benchmark/
    └── LoadBenchmark.java            # Timing, memory tracking, formatted report

src/test/java/com/iso20022/pain/
├── SampleGenerationTest.java         # JUnit 5: Type D (valid) + Type E (invalid CtrlSum)
├── ArrowFileLoadBenchmarkTest.java   # JUnit 5: Arrow IPC → DuckDB load speed benchmark
├── SampleGeneratorRunner.java        # Runnable main: generate by type (a–e)
└── generator/
    ├── PainXmlGenerator.java         # Interface: generate(PainFileSpec, Path) → Path
    ├── PainXmlGeneratorImpl.java     # StAX implementation (honours invalidControlSum)
    ├── TestPainFileSpecs.java        # Test-only spec constants A–E
    └── TestFileGenerator.java        # generate-if-absent + file-complete check
```

## Test Files

| Type | File | Structure | Remittances | Txns/Block | Total Txns | Purpose |
|---|---|---|---|---|---|---|
| A | `pain001_type_a_1x1M.xml` | Fat batch | 1 | 1,000,000 | 1,000,000 | Benchmark |
| B | `pain001_type_b_2x500K.xml` | Two batches | 2 | 500,000 | 1,000,000 | Benchmark |
| C | `pain001_type_c_1Mx1.xml` | Many small | 1,000,000 | 1 | 1,000,000 | Benchmark |
| D | `pain001_type_d_2x100_valid.xml` | Small valid | 2 | 100 | 200 | Unit test |
| E | `pain001_type_e_2x100_invalid_ctrlsum.xml` | Small invalid | 2 | 100 | 200 | Negative test |

> Types A–C are large files (387–1,199 MB) not committed to the repo. Types D–E are fast-generation test files generated automatically by `mvn test`.

## Running

```bash
# Prerequisites: Java 21+, Maven 3.9+, --add-opens for Arrow unsafe allocator

# Parse a pain.001 XML file (must exist — generate it first with mvn test or SampleGeneratorRunner)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn exec:java -Dexec.args="path/to/pain001.xml"

# Generate test sample files (type-d and type-e are fast, A–C are large)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn exec:java -Dexec.mainClass="com.iso20022.pain.SampleGeneratorRunner" \
                -Dexec.args="type-d type-e"

# Run all tests (generates Type D and E files as needed, runs benchmark)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" mvn test

# Run the Arrow load benchmark only
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -Dtest=ArrowFileLoadBenchmarkTest
```

Output Arrow IPC files are written to `src/main/resources/output/` (main pipeline) or `src/test/resources/output/` (tests).

## Validation Framework

The project includes a **chainable validation framework** with support for parallel execution using Java 21 virtual threads. The framework validates ISO 20022 compliance beyond just control sums.

### Architecture

```
ValidationPipeline
├─ MessageValidator      (parallel)  ─┐
├─ RemittanceValidator   (parallel)  ─┤ Execute concurrently
├─ TransactionValidator  (parallel)  ─┘ with virtual threads
└─ ControlSumValidator   (sequential) ─ Runs after parallel group
```

### Validators

1. **MessageValidator** — Validates message-level fields
   - MsgId length ≤ 35 characters
   - InitgPty (Initiating Party) presence
   - CreDtTm (Creation DateTime) required

2. **RemittanceValidator** — Validates payment instruction fields
   - IBAN format: `^[A-Z]{2}[0-9]{2}[A-Z0-9]+$`
   - Payment method required

3. **TransactionValidator** — Validates transaction fields
   - Amount must be positive
   - Creditor name required

4. **ControlSumValidator** — Validates ISO 20022 control sums
   - Remittance-level control sum validation
   - Message-level control sum validation

### Usage

```java
// Standard pipeline with all validators (repository provides SQL access)
try (PaymentRepository repo = new PaymentRepositoryImpl(arrowBatchResult, allocator)) {
    ValidationContext ctx = ValidationPipeline.standard().execute(repo);
}

// Custom pipeline
try (PaymentRepository repo = new PaymentRepositoryImpl(arrowBatchResult, allocator)) {
    ValidationContext ctx = ValidationPipeline.create()
        .add(new MessageValidator())
        .add(new RemittanceValidator())
        .withExecutionMode(ExecutionMode.PARALLEL)
        .execute(repo);
}
```

### Performance

The validation framework adds **172-864 ms** to the pipeline (compared to control sum validation alone) depending on dataset size. This is 1-2% of total pipeline time. The additional time buys comprehensive validation coverage and extensibility.

See [TEST_RESULTS.md](TEST_RESULTS.md) for detailed performance comparison and test methodology.

---

## Study Results

All benchmarks run with **-Xmx1g** heap on a single thread, using **pretty-printed (formatted) XML** files to simulate real-world client input. Arrow off-heap allocator capped at 2 GB. Batch size: 65,536 rows per `VectorSchemaRoot`.

### 1. Storage: XML vs Arrow IPC on Disk

| File            | XML Size (formatted) | Arrow IPC Size | Reduction         |
| --------------- | -------------------- | -------------- | ----------------- |
| Type A (1×1M)   | 622.9 MB             | 170.0 MB       | **72.7% smaller** |
| Type B (2×500K) | 622.9 MB             | 170.0 MB       | **72.7% smaller** |
| Type C (1M×1)   | 1,198.9 MB           | 308.6 MB       | **74.3% smaller** |

Arrow IPC files are consistently **72–74% smaller** than the formatted source XML. XML carries enormous per-element tag overhead (`<CdtTrfTxInf>`, `<PmtId>`, `<InstrId>`, etc.) plus indentation whitespace that Arrow eliminates by storing columnar binary data with compact metadata.

Type C’s higher reduction (74.3%) comes from the XML having to repeat debtor/account/agent elements for every one of its 1M remittance blocks — overhead that Arrow’s columnar layout absorbs efficiently.

### 2. Memory Efficiency

| File   | XML Size   | Arrow Off-Heap | Java Heap | Combined Peak | Combined vs XML |
| ------ | ---------- | -------------- | --------- | ------------- | --------------- |
| Type A | 622.9 MB   | 279.3 MB       | 136.9 MB  | 416.2 MB      | −33.2%          |
| Type B | 622.9 MB   | 279.3 MB       | 31.9 MB   | 311.2 MB      | −50.0%          |
| Type C | 1,198.9 MB | 493.5 MB       | 278.8 MB  | 772.3 MB      | −35.6%          |

**Arrow off-heap memory alone:**

| File   | XML Size   | Arrow Off-Heap | Off-Heap vs XML Size |
| ------ | ---------- | -------------- | -------------------- |
| Type A | 622.9 MB   | 279.3 MB       | **55% less**         |
| Type B | 622.9 MB   | 279.3 MB       | **55% less**         |
| Type C | 1,198.9 MB | 493.5 MB       | **59% less**         |

The Arrow columnar representation in off-heap memory is **55–59% smaller** than the formatted XML file size. Java heap usage comes from the StAX parser's working set (StringBuilder, character buffers) and validation HashMaps — not from Arrow itself.

All test files fit comfortably within a **1 GB heap limit**, with the worst case (Type C) using 772 MB combined — **25% headroom** under the 1 GB cap.

### 3. Parse Throughput (CPU)

| File            | Rows      | Parse Time | Throughput (MB/s) | Throughput (rows/s) |
| --------------- | --------- | ---------- | ----------------- | ------------------- |
| Type A (1×1M)   | 1,000,000 | 9.90 s     | **62.9 MB/s**     | 101,041             |
| Type B (2×500K) | 1,000,000 | 9.06 s     | **68.7 MB/s**     | 110,351             |
| Type C (1M×1)   | 1,000,000 | 18.87 s    | **63.5 MB/s**     | 52,997              |

Parse throughput is **consistent at ~63–69 MB/s** regardless of file structure (note: Type C’s lower rows/sec is because it has 2× the row count — 1M remittances + 1M transactions — while Type A/B only count transaction rows).

The parser uses **boolean depth flags** instead of element stack scanning, giving O(1) context resolution on every XML event. Earlier versions using `ArrayList.contains()` were 1.4× slower on Type C.

### 4. Post-Parse Analytics: The Arrow Payoff

This is where Arrow delivers its primary value. Once data is in columnar form, analytical scans are dramatically faster than re-parsing XML:

| File   | Parse Time | Validation Scan | Scan as % of Parse | Scan Throughput |
| ------ | ---------- | --------------- | ------------------ | --------------- |
| Type A | 9,897 ms   | 154 ms          | **1.6%**           | 6.5 M rows/sec  |
| Type B | 9,062 ms   | 154 ms          | **1.7%**           | 6.5 M rows/sec  |
| Type C | 18,869 ms  | 672 ms          | **3.6%**           | 1.5 M rows/sec  |

**Each additional analytical pass over 1M rows costs only 154–672 ms**, compared to 9–19 seconds to re-parse the XML. This represents a **28–64× speedup** for repeated scans.

Type C’s lower scan throughput (1.5 M vs 6.5 M rows/sec) is due to the HashMap lookup overhead of joining 1M distinct `pmt_inf_id` keys — a characteristic of the data shape, not an Arrow limitation.

### 5. Arrow IPC Write Speed

| File   | Arrow Size | Write Time | Write Speed  |
| ------ | ---------- | ---------- | ------------ |
| Type A | 170.0 MB   | 261 ms     | **651 MB/s** |
| Type B | 170.0 MB   | 270 ms     | **630 MB/s** |
| Type C | 308.6 MB   | 441 ms     | **700 MB/s** |

Arrow IPC writes at **~650–700 MB/s** — essentially memcpy speed — because Arrow's in-memory format **is** the on-disk format. No serialization step is needed.

### 6. End-to-End Pipeline Timing

| File   | Parse     | Validate | Write  | **Total**     |
| ------ | --------- | -------- | ------ | ------------- |
| Type A | 9,897 ms  | 154 ms   | 261 ms | **10,312 ms** |
| Type B | 9,062 ms  | 154 ms   | 270 ms | **9,486 ms**  |
| Type C | 18,869 ms | 672 ms   | 441 ms | **19,982 ms** |

Parsing dominates the pipeline (93–96% of total time). Validation and IPC export together account for only 4–7% of wall time.

### Summary

| Metric                    | Result                                             |
| ------------------------- | -------------------------------------------------- |
| **Disk storage**          | 72–74% smaller than formatted XML                  |
| **Off-heap memory**       | 55–59% less than formatted XML file size           |
| **Post-parse scan speed** | 1.5–6.5 M rows/sec (28–64× faster than re-parsing) |
| **IPC write speed**       | ~650–700 MB/s (zero-copy format)                   |
| **1 GB heap feasibility** | ✓ All files pass — worst case uses 772 MB combined |

### Conclusion

The **real ROI of Arrow is amortised over multiple analytical passes**. The initial XML → Arrow parse is a one-time cost (~63 MB/s on formatted XML). After that, every additional scan, aggregation, or validation runs at columnar speeds (millions of rows/sec) with zero deserialization overhead — and the data can be persisted to IPC files at 650+ MB/s for later reuse without any re-parsing at all.

For a single read-once-discard workload, Arrow adds complexity without benefit. For workloads that **parse once then query repeatedly** — audit, reconciliation, regulatory checks, analytics — Arrow turns a 10–19 second re-parse into a 154–672 ms scan. That’s the payoff.

---

## Arrow File Sharing via S3

One of Arrow's most compelling production use cases is **sharing pre-parsed data across applications via object storage (e.g. AWS S3, GCS, Azure Blob)** — instead of passing raw XML or JSON and forcing each consumer to re-parse.

### The Traditional Approach (XML/JSON over S3)

```
Producer App                        Consumer App A
  generate pain.001 XML  ──S3──▶   download XML (387–1,199 MB)
                                    parse XML:    9–19 seconds
                                    process data

                         ──S3──▶   Consumer App B: same cost again
                         ──S3──▶   Consumer App C: same cost again
```

**Each consumer pays the full parse cost independently**, even though the data is identical.

### The Arrow IPC Approach

```
Producer App                        Consumer App A
  parse XML once                    download .arrow files (170–309 MB, 55–74% smaller)
  export Arrow IPC  ──S3──▶         load into DuckDB:  ~50–200 ms  ← benchmark result
  3 × .arrow files                  run SQL analytics

                    ──S3──▶         Consumer App B: same ~50–200 ms
                    ──S3──▶         Consumer App C: same ~50–200 ms
```

### Measured Performance Comparison

| Metric                        | XML/JSON (re-parse)          | Arrow IPC (load from file)   | Speedup     |
|-------------------------------|------------------------------|------------------------------|-------------|
| Transfer size (Type A/B)      | 387 MB (compact)             | 170 MB                       | 2.3× smaller |
| Transfer size (Type C)        | 760 MB (compact)             | 309 MB                       | 2.5× smaller |
| Load/parse time (1M rows)     | 9–19 seconds                 | 50–200 ms                    | **50–200×** |
| CPU per consumer              | Full StAX parse              | DuckDB bulk vectorised load  | Minimal     |
| Schema enforcement            | Manual, error-prone          | Built into Arrow schema      | Automatic   |
| Type fidelity (Decimal128)    | String-to-BigDecimal cast    | Native columnar Decimal128   | Zero-cost   |

### Why Arrow IPC Is Better Than JSON/CSV for This Use Case

1. **Schema is self-describing** — no schema registry needed; the `.arrow` file carries its schema.
2. **Zero deserialization** — Arrow's in-memory format IS the on-disk format; DuckDB reads buffers directly.
3. **Language-agnostic** — Python (PyArrow/DuckDB), Go, Rust, C++, and Java consumers all read the same file.
4. **Columnar compression** — repeated strings (IBANs, BICs, currency codes) compress well in columnar layout.
5. **Predicate pushdown** — DuckDB can push filters into Arrow reads, scanning only relevant columns/batches.

### S3 Cost Implications

At $0.023/GB (S3 Standard), for 1,000 pain.001 files per day at Type A/B size:

| Format | Storage per day | Transfer to 3 consumers/day |
|---|---|---|
| XML (compact) | 387 GB → $8.90/day | 3 × 387 GB → $26.70/day |
| Arrow IPC | 170 GB → $3.91/day | 3 × 170 GB → $11.73/day |
| **Saving** | **$4.99/day** | **$14.97/day** |

Plus the compute savings: 9 seconds × 3 consumers × 1,000 files = **7.5 CPU-hours/day saved** per consumer application.

> **The benchmark for this is in `ArrowFileLoadBenchmarkTest`** — it simulates a downstream consumer receiving Arrow IPC files and loading them directly into DuckDB, measuring real load latency on your hardware.


## Parser Optimisation Notes

The initial StAX parser implementation used an `ArrayList<String>` element stack with `stackContains()` checks to determine parsing context. This performed 7 linear scans per `END_ELEMENT` event. For Type C with ~20M end-element events, this produced ~140M `String.equals()` calls.

**Fix:** Replaced the element stack with **boolean depth flags** (`inGrpHdr`, `inPmtInf`, `inCdtTrfTxInf`, etc.) that toggle O(1) on start/end element events. Also deferred `toString().trim()` calls to only the branches that actually consume text content.

| Metric                       | Before        | After         | Improvement     |
| ---------------------------- | ------------- | ------------- | --------------- |
| Type C parse time            | 20.2 s        | 18.9 s        | **1.5× faster** |
| Type C throughput            | 37.6 MB/s     | 63.5 MB/s     | **+69%**        |
| Throughput variance (A vs C) | 56 vs 38 MB/s | 63 vs 64 MB/s | **Normalised**  |

## License

This is a study/research project. Use at your own discretion.
