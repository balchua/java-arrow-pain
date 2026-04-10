# Testing Guide

This document describes the testing infrastructure for the PGW multi-module project.

---

## Quick Start

```bash
# Run the full standardized test suite (generates samples, runs all benchmarks)
./run_validation_tests.sh
```

Results are saved to `test-results/test_run_YYYYMMDD_HHMMSS.log`.

---

## Module Layout

```
pain001-arrow-loader/        ← parent POM
├── pgw-common/              ← shared Arrow infrastructure (parser, schema, generator, benchmark)
│   └── src/test/java/com/pgw/generator/
│       ├── TestPainFileSpecs.java    # type constants A–G
│       └── TestFileGenerator.java   # generate-if-absent + tail-check
├── pgw-domain/              ← pure-Java domain (VOs, exceptions, models) — no tests
├── pgw-ingestor/            ← XML → DuckDB → Arrow IPC
│   └── src/test/java/com/pgw/
│       ├── ParsePipelineTest.java         # StAX parser correctness
│       ├── StreamingPipelineTest.java     # streaming memory, DuckDB counts, ArrowIpc round-trip
│       ├── MemoryLeakVerificationTest.java # 50-iteration leak check (0 bytes leaked)
│       └── IngestionBenchmarkTest.java    # DuckDB ingest benchmark, Types A–G
├── pgw-ingestor-pure-arrow/ ← XML → Arrow IPC only (no DuckDB at ingest time)
│   └── src/test/java/com/pgw/purearrow/
│       ├── PureArrowParsePipelineTest.java        # correctness: Types D & E
│       ├── PureArrowStreamingPipelineTest.java    # memory, row counts, IPC round-trip
│       ├── PureArrowMemoryLeakVerificationTest.java # 50-iteration leak check (0 bytes)
│       ├── PureArrowIngestionBenchmarkTest.java   # pure-Arrow benchmark, Types A–G
│       └── PipelineComparisonBenchmarkTest.java   # DuckDB vs Pure Arrow, Types A–G
└── pgw-validator/           ← validation domain + App + downstream tests
    └── src/test/java/com/pgw/
        ├── ValidationTest.java             # Type D passes, Type E fails correctly
        ├── ArrowFileLoadBenchmarkTest.java # .arrow → DuckDB load speed, all types
        └── ValidationBenchmarkTest.java    # load + SQL validation separated, all types
```

---

## Running Tests

### Build all modules first

```bash
export JAVA_HOME=/usr/lib/jvm/temurin-25-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Build and install all 5 modules
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn clean install -DskipTests
```

### Run all tests

```bash
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test
```

### Run a specific module

```bash
# pgw-common (shared infrastructure — TestFileGenerator only)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-common

# pgw-ingestor (DuckDB pipeline correctness + benchmark)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor

# pgw-ingestor-pure-arrow (pure-Arrow pipeline + comparison benchmark)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor-pure-arrow

# pgw-validator (domain validation + downstream load/validation benchmarks)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator
```

### Run a specific test class

```bash
# DuckDB pipeline: parse + insert correctness
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor -Dtest=ParsePipelineTest

# DuckDB pipeline: streaming memory footprint + Arrow export
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor -Dtest=StreamingPipelineTest

# DuckDB pipeline: 50-iteration memory leak check
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor -Dtest=MemoryLeakVerificationTest

# DuckDB pipeline: ingestion benchmark (Types A–G)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor -Dtest=IngestionBenchmarkTest

# Pure-Arrow pipeline: parse correctness (Types D + E)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor-pure-arrow -Dtest=PureArrowParsePipelineTest

# Pure-Arrow pipeline: streaming memory footprint
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor-pure-arrow -Dtest=PureArrowStreamingPipelineTest

# Pure-Arrow pipeline: 50-iteration memory leak check
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-ingestor-pure-arrow -Dtest=PureArrowMemoryLeakVerificationTest

# Pure-Arrow pipeline: benchmark (Types A–G, no DuckDB)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor-pure-arrow -Dtest=PureArrowIngestionBenchmarkTest

# *** DuckDB vs Pure Arrow side-by-side comparison ***
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-ingestor-pure-arrow -Dtest=PipelineComparisonBenchmarkTest

# Validation correctness (Type D passes, Type E fails correctly)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-validator -Dtest=ValidationTest

# Arrow → DuckDB load benchmark (no validation)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator -Dtest=ArrowFileLoadBenchmarkTest

# Validation stage benchmark (load + SQL validation separated)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" \
  mvn test -pl pgw-validator -Dtest=ValidationBenchmarkTest
```

---

## Test Files

| Type | File | Remittances | Txns/Block | Total Txns | Purpose |
|------|------|-------------|------------|------------|---------|
| A | `pain001_type_a_1x1M.xml` | 1 | 1,000,000 | 1,000,000 | Fat batch benchmark |
| B | `pain001_type_b_2x500K.xml` | 2 | 500,000 | 1,000,000 | Multi-batch benchmark |
| C | `pain001_type_c_1Mx1.xml` | 1,000,000 | 1 | 1,000,000 | Adversarial (max overhead) |
| D | `pain001_type_d_2x100_valid.xml` | 2 | 100 | 200 | Valid — fast unit test |
| E | `pain001_type_e_2x100_invalid_ctrlsum.xml` | 2 | 100 | 200 | Invalid control sum — negative test |
| F | `pain001_type_f_1x2M.xml` | 1 | 2,000,000 | 2,000,000 | Large-scale benchmark |
| G | `pain001_type_g_1x4M.xml` | 1 | 4,000,000 | 4,000,000 | Extreme-scale benchmark |

> Types A–C, F, G (~516 MB – 2.9 GB) are not committed to the repo.
> They are generated on first test run by `TestFileGenerator.generateIfAbsent()`.
> Types D–E are small (<110 KB) and generated automatically by `mvn test`.

---

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `PAIN_PERSISTENCE_MODE` | `local` | `local` or `s3` |
| `PAIN_LOCAL_OUTPUT_DIR` | _(module resource dir)_ | Local output directory for Arrow IPC Stream files |
| `PAIN_S3_BUCKET` | _(required for s3)_ | Target S3 bucket name |
| `PAIN_S3_KEY_PREFIX` | `pain001` | S3 key prefix |

---

## Before/After Comparison Workflow

```bash
# 1. Establish baseline BEFORE changes
./run_validation_tests.sh
BASELINE=test-results/test_run_$(date +%Y%m%d_%H%M%S).log

# 2. Make your code changes

# 3. Run AFTER changes
./run_validation_tests.sh
AFTER=test-results/test_run_$(date +%Y%m%d_%H%M%S).log

# 4. Compare
diff -u $BASELINE $AFTER | grep -E "^[+-].*ms|Validation|Benchmark|Speedup"
```

### Key Metrics to Watch

| Metric | Where | Example |
|--------|-------|---------|
| DuckDB parse+insert throughput | `IngestionBenchmarkTest` | `15,843 ms for 1M rows` |
| Pure-Arrow parse throughput | `PureArrowIngestionBenchmarkTest` | `3,421 ms for 1M rows` |
| Speedup (Pure Arrow vs DuckDB) | `PipelineComparisonBenchmarkTest` | `4.80×` |
| Peak off-heap (DuckDB path) | `IngestionBenchmarkTest` | `~31 MB` |
| Peak off-heap (pure-Arrow path) | `PureArrowIngestionBenchmarkTest` | `~31 MB` |
| Memory leak | `*MemoryLeakVerificationTest` | `0 bytes leaked` |
| Validation time | `ValidationBenchmarkTest` | `80 ms for 1M rows` |
| DuckDB load time | `ArrowFileLoadBenchmarkTest` | `635 ms for 1M rows` |

---

## Updating TEST_RESULTS.md

After running benchmarks, add a new section to [TEST_RESULTS.md](TEST_RESULTS.md):

```markdown
## Test Run: YYYY-MM-DD

### Changes
- Brief description

### DuckDB Pipeline Results

| Type | Parse+Ins ms | Export ms | Peak Off-Heap | Arrow MB | Tx Rows |
|------|-------------|-----------|--------------|----------|---------|
| A    | 15,843      | 578       | 31 MB        | 294.6    | 1,000,000 |
| ...  |             |           |              |          |         |

### Pure-Arrow Pipeline Results

| Type | Parse ms | Peak Off-Heap | Arrow MB | Tx Rows |
|------|----------|--------------|----------|---------|
| A    | 3,421    | 31 MB        | 294.6    | 1,000,000 |
| ...  |          |              |          |         |

### DuckDB vs Pure Arrow Comparison

| Type | DuckDB total ms | Pure Arrow ms | Speedup |
|------|-----------------|---------------|---------|
| A    | 16,421          | 3,421         | 4.80×   |

### Conclusions
- Summary
```

---

## Troubleshooting

**`error: invalid target release: 25`** — Use Java 25:
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-25-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

**`OutOfMemoryError`** — Increase heap:
```bash
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" mvn test
```

**Tests skip large file types (A–C, F, G)** — They generate on first run. Types A–C take ~14–23 seconds to generate, F–G take ~30–90 seconds. Benchmarks for these types only run after their files exist.

**`Could not find artifact com.pgw:pgw-common:jar:1.0.0`** — Install all modules first:
```bash
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn clean install -DskipTests
```

---

**Last Updated:** 2026-04-10
**Architecture:** Multi-module Maven (5 modules: `pgw-common`, `pgw-domain`, `pgw-ingestor`, `pgw-ingestor-pure-arrow`, `pgw-validator`)
**Package root:** `com.pgw`
