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
│   └── SampleFileSpec.java           # Test file specifications (Type A/B/C)
├── validation/
│   └── ControlSumValidator.java      # Validates control sums by scanning Arrow vectors
└── benchmark/
    └── LoadBenchmark.java            # Timing, memory tracking, formatted report
```

## Test Files

Four XML files exercise different structural shapes of the pain.001 schema, all producing **1 million transactions**:

| File                        | Structure   | Remittances | Txns/Remittance | Total Txns | XML Size |
| --------------------------- | ----------- | ----------- | --------------- | ---------- | -------- |
| `pain001_test_10.xml`       | Trivial     | 1           | 10              | 10         | 10 KB    |
| `pain001_type_a_1x1M.xml`   | Fat batch   | 1           | 1,000,000       | 1,000,000  | 387 MB   |
| `pain001_type_b_2x500K.xml` | Two batches | 2           | 500,000         | 1,000,000  | 387 MB   |
| `pain001_type_c_1Mx1.xml`   | Many small  | 1,000,000   | 1               | 1,000,000  | 760 MB   |

Type C is intentionally adversarial — it maximises remittance-level overhead (debtor info, account, agent repeated per transaction) and produces the largest XML per transaction.

## Running

```bash
# Prerequisites: Java 17+, Maven 3.9+

# Generate all 3 sample XML files + parse + validate + export
mvn exec:java

# Parse a single file
mvn exec:java -Dexec.args="src/main/resources/sample-data/pain001_type_a_1x1M.xml"

# Generate sample files only (no parsing)
mvn exec:java -Dexec.args="generate"

# Run with constrained heap (1 GB)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx1g" \
  mvn exec:java -Dexec.args="src/main/resources/sample-data/pain001_type_a_1x1M.xml"
```

Output Arrow IPC files are written to `src/main/resources/output/`.

---

## Study Results

All benchmarks run with **-Xmx1g** heap on a single thread. Arrow off-heap allocator capped at 2 GB. Batch size: 65,536 rows per `VectorSchemaRoot`.

### 1. Storage: XML vs Arrow IPC on Disk

| File            | XML Size | Arrow IPC Size | Reduction         |
| --------------- | -------- | -------------- | ----------------- |
| Type A (1×1M)   | 387.4 MB | 170.0 MB       | **56.1% smaller** |
| Type B (2×500K) | 387.4 MB | 170.0 MB       | **56.1% smaller** |
| Type C (1M×1)   | 760.2 MB | 308.6 MB       | **59.4% smaller** |

Arrow IPC files are consistently **56–59% smaller** than the source XML. XML carries enormous per-element tag overhead (`<CdtTrfTxInf>`, `<PmtId>`, `<InstrId>`, etc.) that Arrow eliminates by storing columnar binary data with compact metadata.

Type C's higher reduction (59.4%) comes from the XML having to repeat debtor/account/agent elements for every one of its 1M remittance blocks — overhead that Arrow's columnar layout absorbs efficiently.

### 2. Memory Efficiency

| File   | XML Size | Arrow Off-Heap | Java Heap | Combined Peak | Combined vs XML |
| ------ | -------- | -------------- | --------- | ------------- | --------------- |
| Type A | 387.4 MB | 279.3 MB       | 138.5 MB  | 417.8 MB      | +7.8%           |
| Type B | 387.4 MB | 279.3 MB       | 107.0 MB  | 386.2 MB      | −0.3%           |
| Type C | 760.2 MB | 493.5 MB       | 215.6 MB  | 709.1 MB      | −6.7%           |

**Arrow off-heap memory alone:**

| File   | XML Size | Arrow Off-Heap | Off-Heap vs XML Size |
| ------ | -------- | -------------- | -------------------- |
| Type A | 387.4 MB | 279.3 MB       | **28% less**         |
| Type B | 387.4 MB | 279.3 MB       | **28% less**         |
| Type C | 760.2 MB | 493.5 MB       | **35% less**         |

The Arrow columnar representation in off-heap memory is **28–35% smaller** than the raw XML file size. Java heap usage comes from the StAX parser's working set (StringBuilder, character buffers) and validation HashMaps — not from Arrow itself.

All test files fit comfortably within a **1 GB heap limit**, with the worst case (Type C) using 709 MB combined — **31% headroom** under the 1 GB cap.

### 3. Parse Throughput (CPU)

| File            | Rows      | Parse Time | Throughput (MB/s) | Throughput (rows/s) |
| --------------- | --------- | ---------- | ----------------- | ------------------- |
| Type A (1×1M)   | 1,000,000 | 6.92 s     | **56.0 MB/s**     | 144,509             |
| Type B (2×500K) | 1,000,000 | 7.36 s     | **52.7 MB/s**     | 135,925             |
| Type C (1M×1)   | 1,000,000 | 13.62 s    | **55.8 MB/s**     | 73,416              |

Parse throughput is **consistent at ~53–56 MB/s** regardless of file structure (note: Type C's lower rows/sec is because it has 2× the row count — 1M remittances + 1M transactions — while Type A/B only count transaction rows).

The parser uses **boolean depth flags** instead of element stack scanning, giving O(1) context resolution on every XML event. Earlier versions using `ArrayList.contains()` were 1.4× slower on Type C.

### 4. Post-Parse Analytics: The Arrow Payoff

This is where Arrow delivers its primary value. Once data is in columnar form, analytical scans are dramatically faster than re-parsing XML:

| File   | Parse Time | Validation Scan | Scan as % of Parse | Scan Throughput |
| ------ | ---------- | --------------- | ------------------ | --------------- |
| Type A | 6,920 ms   | 220 ms          | **3.2%**           | 4.6 M rows/sec  |
| Type B | 7,357 ms   | 211 ms          | **2.9%**           | 4.7 M rows/sec  |
| Type C | 13,621 ms  | 708 ms          | **5.2%**           | 1.4 M rows/sec  |

**Each additional analytical pass over 1M rows costs only 200–700 ms**, compared to 7–14 seconds to re-parse the XML. This represents a **20–34× speedup** for repeated scans.

Type C's lower scan throughput (1.4 M vs 4.7 M rows/sec) is due to the HashMap lookup overhead of joining 1M distinct `pmt_inf_id` keys — a characteristic of the data shape, not an Arrow limitation.

### 5. Arrow IPC Write Speed

| File   | Arrow Size | Write Time | Write Speed  |
| ------ | ---------- | ---------- | ------------ |
| Type A | 170.0 MB   | 276 ms     | **616 MB/s** |
| Type B | 170.0 MB   | 277 ms     | **614 MB/s** |
| Type C | 308.6 MB   | 515 ms     | **599 MB/s** |

Arrow IPC writes at **~600 MB/s** — essentially memcpy speed — because Arrow's in-memory format **is** the on-disk format. No serialization step is needed.

### 6. End-to-End Pipeline Timing

| File   | Parse     | Validate | Write  | **Total**     |
| ------ | --------- | -------- | ------ | ------------- |
| Type A | 6,920 ms  | 220 ms   | 276 ms | **7,416 ms**  |
| Type B | 7,357 ms  | 211 ms   | 277 ms | **7,845 ms**  |
| Type C | 13,621 ms | 708 ms   | 515 ms | **14,844 ms** |

Parsing dominates the pipeline (88–93% of total time). Validation and IPC export together account for only 7–12% of wall time.

### Summary

| Metric                    | Result                                             |
| ------------------------- | -------------------------------------------------- |
| **Disk storage**          | 56–59% smaller than XML                            |
| **Off-heap memory**       | 28–35% less than XML file size                     |
| **Post-parse scan speed** | 1.4–4.7 M rows/sec (20–34× faster than re-parsing) |
| **IPC write speed**       | ~600 MB/s (zero-copy format)                       |
| **1 GB heap feasibility** | ✓ All files pass — worst case uses 709 MB combined |

### Conclusion

The **real ROI of Arrow is amortised over multiple analytical passes**. The initial XML → Arrow parse is a one-time cost (~53 MB/s). After that, every additional scan, aggregation, or validation runs at columnar speeds (millions of rows/sec) with zero deserialization overhead — and the data can be persisted to IPC files at 600 MB/s for later reuse without any re-parsing at all.

For a single read-once-discard workload, Arrow adds complexity without benefit. For workloads that **parse once then query repeatedly** — audit, reconciliation, regulatory checks, analytics — Arrow turns a 7-second re-parse into a 200 ms scan. That's the payoff.

## Parser Optimisation Notes

The initial StAX parser implementation used an `ArrayList<String>` element stack with `stackContains()` checks to determine parsing context. This performed 7 linear scans per `END_ELEMENT` event. For Type C with ~20M end-element events, this produced ~140M `String.equals()` calls.

**Fix:** Replaced the element stack with **boolean depth flags** (`inGrpHdr`, `inPmtInf`, `inCdtTrfTxInf`, etc.) that toggle O(1) on start/end element events. Also deferred `toString().trim()` calls to only the branches that actually consume text content.

| Metric                       | Before        | After         | Improvement     |
| ---------------------------- | ------------- | ------------- | --------------- |
| Type C parse time            | 20.2 s        | 13.6 s        | **1.5× faster** |
| Type C throughput            | 37.6 MB/s     | 55.8 MB/s     | **+48%**        |
| Throughput variance (A vs C) | 56 vs 38 MB/s | 56 vs 56 MB/s | **Normalised**  |

## License

This is a study/research project. Use at your own discretion.
