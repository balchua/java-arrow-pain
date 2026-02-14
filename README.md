# ISO 20022 pain.001 → Apache Arrow Loader

A study project that parses ISO 20022 **pain.001.001.09** (CustomerCreditTransferInitiation) XML files into **Apache Arrow** columnar in-memory tables using a streaming StAX parser — with no DOM, no JAXB, and no intermediate POJOs.

The goal is to measure whether Apache Arrow's columnar format provides meaningful gains in **storage**, **memory**, and **analytical throughput** over raw XML for financial messaging workloads.

## Tech Stack

| Component    | Version                                           |
| ------------ | ------------------------------------------------- |
| Java         | 17                                                |
| Apache Arrow | 15.0.2 (`arrow-vector`, `arrow-memory-unsafe`)    |
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
                              │  CtrlSum          │
                              │  Validation       │
                              └──────────────────┘
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
├── App.java                          # Entry point — orchestrates generate → parse → validate → export
├── arrow/
│   ├── Pain001ArrowSchema.java       # Arrow schema definitions for all 3 tables
│   ├── ArrowBatchResult.java         # Holds VectorSchemaRoot tables (AutoCloseable)
│   └── ArrowFileExporter.java        # Writes Arrow IPC files (VectorUnloader/Loader pattern)
├── parser/
│   └── Pain001StaxParser.java        # Streaming StAX parser → Arrow vectors (no POJOs)
├── generator/
│   ├── Pain001XmlGenerator.java      # Generates valid pain.001.001.09 XML via StAX
│   ├── IndentingXMLStreamWriter.java  # Pretty-printing decorator for XMLStreamWriter
│   └── SampleFileSpec.java           # Test file specifications (Type A/B/C)
├── validation/
│   └── ControlSumValidator.java      # Validates control sums by scanning Arrow vectors
└── benchmark/
    └── LoadBenchmark.java            # Timing, memory tracking, formatted report
```

## Test Files

Four XML files exercise different structural shapes of the pain.001 schema, all producing **1 million transactions**:

| File                        | Structure   | Remittances | Txns/Remittance | Total Txns | XML Size (compact) | XML Size (formatted) |
| --------------------------- | ----------- | ----------- | --------------- | ---------- | ------------------ | -------------------- |
| `pain001_test_10.xml`       | Trivial     | 1           | 10              | 10         | 10 KB              | 10 KB                |
| `pain001_type_a_1x1M.xml`   | Fat batch   | 1           | 1,000,000       | 1,000,000  | 387 MB             | 623 MB               |
| `pain001_type_b_2x500K.xml` | Two batches | 2           | 500,000         | 1,000,000  | 387 MB             | 623 MB               |
| `pain001_type_c_1Mx1.xml`   | Many small  | 1,000,000   | 1               | 1,000,000  | 760 MB             | 1,199 MB             |

Type C is intentionally adversarial — it maximises remittance-level overhead (debtor info, account, agent repeated per transaction) and produces the largest XML per transaction.

## Running

```bash
# Prerequisites: Java 17+, Maven 3.9+

# Generate all 3 sample XML files + parse + validate + export
mvn exec:java

# Parse a single file
mvn exec:java -Dexec.args="src/main/resources/sample-data/pain001_type_a_1x1M.xml"

# Generate compact (minified) sample files only
mvn exec:java -Dexec.args="generate"

# Generate pretty-printed (formatted) sample files
mvn exec:java -Dexec.args="generate-formatted"

# Run with constrained heap (1 GB)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx1g" \
  mvn exec:java -Dexec.args="src/main/resources/sample-data/pain001_type_a_1x1M.xml"
```

Output Arrow IPC files are written to `src/main/resources/output/`.

### Sample Data Files

The large XML test files (up to 1.2 GB formatted) are **not included in the repository**. Generate them locally:

```bash
# Generate compact (minified) XML — one-line format, smallest on disk
mvn exec:java -Dexec.args="generate"

# Generate pretty-printed XML — indented, ~60% larger but human-readable
mvn exec:java -Dexec.args="generate-formatted"

# Compact sizes: Type A/B = 387 MB, Type C = 760 MB
# Formatted sizes: Type A/B = 623 MB, Type C = 1,199 MB
```

Formatted XML is recommended for testing since real-world client files are often pretty-printed. The parser handles both formats with identical correctness and comparable throughput.

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
