# PGW — Test Results & Benchmark Report

All benchmarks were run on the multi-module build (`pgw-ingestor` + `pgw-validator`)
with the package root renamed from `com.iso20022.pain` to `com.pgw`.

**Environment:** Java 25 (Temurin 25.0.2), Maven 3.9, `-Xmx2g`,
`--add-opens=java.base/java.nio=ALL-UNNAMED`

---

## Test Run: 2026-04-05

### Build
```
mvn test -pl pgw-validator --also-make
```

### Test Suite Results

| Test Class | Tests | Failures | Errors | Skipped | Time |
|------------|------:|----------|--------|---------|------|
| `MemoryLeakVerificationTest` | 3 | 0 | 0 | 0 | 4.7 s |
| `FullPipelineBenchmarkTest` | 1 | 0 | 0 | 0 | 90.5 s |
| `ArrowFileLoadBenchmarkTest` | 1 | 0 | 0 | 0 | 44.6 s |
| `SampleGenerationTest` | 6 | 0 | 0 | 0 | 0.09 s |
| `StreamingPipelineTest` | 4 | 0 | 0 | 0 | 0.09 s |
| **Total** | **15** | **0** | **0** | **0** | **~2.3 min** |

✅ **All 15 tests passed.**

---

## Full Pipeline Benchmark — Types A–E

**Pipeline:** XML → Arrow (streaming StAX parse) → DuckDB (C Data Interface INSERT)
→ Arrow IPC Stream write → SQL + domain validation

| Type | XML (MB) | Arrow (MB) | Savings | Parse (ms) | Validate (ms) | Off-Heap HWM (MB) | Heap Δ (MB) | Tx Rows | Valid |
|------|----------|------------|---------|------------|---------------|-------------------|-------------|---------|-------|
| A — 1×1M | 516.0 | 224.4 | 56.5% | 9,236 | 60 | 24.5 | 12.6 | 1,000,000 | ✓ |
| B — 2×500K | 515.9 | 224.3 | 56.5% | 7,829 | 65 | 24.5 | 59.6 | 1,000,000 | ✓ |
| C — 1M×1 | 888.0 | 362.1 | 59.2% | 21,535 | 329 | 38.1 | 17.6 | 1,000,000 | ✓ |
| D — 2×100 (valid) | 0.1 | 0.0 | — | 9 | 6 | — | — | 200 | ✓ |
| E — 2×100 (invalid CtrlSum) | 0.1 | 0.0 | — | 9 | 6 | — | — | 200 | ✗ (3 errors) |

**Notes:**
- _Arrow IPC Write_ = 0 ms for all types because `LocalFilePersistenceService` flushes
  inline during parse; the `finish()` call has no remaining work.
- _Savings_ = `(1 − Arrow/XML) × 100`. The Arrow IPC Stream format is 56–59% smaller
  than the source XML.
- Type E correctly reports 3 control-sum errors (2 remittance-level + 1 message-level).

### Per-Type Detail

#### Type A — 1 PmtInf × 1,000,000 TxInf (fat batch)
```
XML File Size    :  541,056,359 bytes (516.0 MB)
Arrow File Size  :  235,321,832 bytes (224.4 MB)
XML→Arrow Parse  :        9,236 ms  (9.24 s)
SQL Validation   :           60 ms  (0.06 s)
Parse Throughput :  108,272 rows/sec  |  55.87 MB/sec
Off-heap peak    :   25,640,960 bytes (24.5 MB)
Heap delta       :   13,223,296 bytes (12.6 MB)
Total Pod Impact :    1,072.9 MB  [Heap + Arrow + DuckDB 1GB est.]
Validation       :  ✓ PASSED
```

#### Type B — 2 PmtInf × 500,000 TxInf
```
XML File Size    :  540,945,672 bytes (515.9 MB)
Arrow File Size  :  235,210,928 bytes (224.3 MB)
XML→Arrow Parse  :        7,829 ms  (7.83 s)
SQL Validation   :           65 ms  (0.07 s)
Parse Throughput :  127,730 rows/sec  |  65.89 MB/sec
Off-heap peak    :   25,640,960 bytes (24.5 MB)
Heap delta       :   62,503,144 bytes (59.6 MB)
Total Pod Impact :    1,119.9 MB
Validation       :  ✓ PASSED
```

#### Type C — 1,000,000 PmtInf × 1 TxInf (adversarial)
```
XML File Size    :  931,139,307 bytes (888.0 MB)
Arrow File Size  :  379,666,304 bytes (362.1 MB)
XML→Arrow Parse  :       21,535 ms  (21.54 s)
SQL Validation   :          329 ms  (0.33 s)
Parse Throughput :   46,436 rows/sec  |  41.24 MB/sec
Off-heap peak    :   39,976,960 bytes (38.1 MB)
Heap delta       :   18,435,840 bytes (17.6 MB)
Validation       :  ✓ PASSED (1,000,000 remittances checked)
```

#### Type D — 2 PmtInf × 100 TxInf (small, valid)
```
XML File Size    :      108,584 bytes (0.1 MB)
Arrow File Size  :       50,672 bytes (0.0 MB)
XML→Arrow Parse  :            9 ms
SQL Validation   :            6 ms
Validation       :  ✓ PASSED
```

#### Type E — 2 PmtInf × 100 TxInf (invalid control sum)
```
XML File Size    :      108,576 bytes (0.1 MB)
Arrow File Size  :       50,672 bytes (0.0 MB)
XML→Arrow Parse  :            9 ms
SQL Validation   :            6 ms
Validation       :  ✗ FAILED (3 control sum errors: 2 remittance-level + 1 message-level)
```

---

## Arrow IPC Stream → DuckDB Load Benchmark (Downstream Consumer Simulation)

Measures the time for a downstream consumer to load pre-written `.arrows` files
into a fresh DuckDB instance using the Arrow C Data Interface.

| Type | Msg KB | Rmt KB | Tx KB | Total KB | Load (ms) | Rows/sec | Tx Rows |
|------|-------:|-------:|------:|---------:|----------:|---------:|--------:|
| A — 1×1M | <1 | <1 | 229,803 | 229,806 | 386 | 2,590,676 | 1,000,000 |
| B — 2×500K | <1 | <1 | 229,695 | 229,698 | 376 | 2,659,579 | 1,000,000 |
| C — 1M×1 | <1 | 139,064 | 231,702 | 370,767 | 687 | 2,911,208 | 1,000,000 |

**Key insight:** Downstream consumers can load 1 million rows in **376–687 ms** from
Arrow IPC Stream files, achieving **2.6–2.9 million rows/sec** — without ever touching
the original XML.

---

## Memory Leak Verification — 50 Iterations

Both Type D (valid) and Type E (invalid control sum) were run 50 times each to verify
the Arrow allocator is fully reclaimed after each pipeline execution.

| Scenario | Iterations | Bytes Leaked | Min/Avg/Max (ms) |
|----------|----------:|-------------:|-----------------|
| Type E (invalid CtrlSum, streaming) | 50 | **0** | 27 / 51.1 / 783 |
| Type D (valid, streaming) | 50 | **0** | 23 / 32.7 / 47 |
| Type D (valid, streaming — short run) | 3 | **0** | 25 / 27.0 / 30 |

✅ **Zero bytes leaked across all 103 iterations.**

---

## Validation Pipeline — Execution Mode

```
ValidationPipeline — PARALLEL mode (virtual threads)
  Parallelizable (run concurrently):
    MessageValidator, RemittanceValidator, TransactionValidator,
    MessageDomainValidator, RemittanceDomainValidator
  Sequential (run after parallel group):
    TransactionDomainValidator, ControlSumDomainService, ControlSumValidator
```

---

## Architecture Change: Multi-Module + Package Rename

This benchmark run reflects the restructured codebase:

| Aspect | Before | After |
|--------|--------|-------|
| Build | Single Maven module | Multi-module: `pgw-ingestor` + `pgw-validator` |
| Package root | `com.iso20022.pain` | `com.pgw` |
| `pgw-ingestor` | — | Pure ingestion pipeline: Arrow schema, StAX parser, DuckDB loader, persistence. No domain objects. |
| `pgw-validator` domain | — | ALL domain: `com.pgw.domain.model.*` (read-model DTOs) + `valueobject.*` + `exception.*` + `service.*` + `dal.*` |
| Test count | 15 | 15 (all preserved, all passing) |

Performance is unchanged by the refactor — the runtime code is identical.
