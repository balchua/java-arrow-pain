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
pain001-arrow-loader/    ← parent POM
├── pgw-ingestor/        ← XML → Arrow → DuckDB (no tests — tested via pgw-validator)
└── pgw-validator/       ← validation domain + App + ALL tests
    └── src/test/java/com/pgw/
        ├── SampleGenerationTest.java       # generates + validates Type D & E
        ├── StreamingPipelineTest.java      # memory footprint, row counts, .arrows output
        ├── ArrowFileLoadBenchmarkTest.java # downstream consumer: .arrows → DuckDB load speed
        ├── MemoryLeakVerificationTest.java # 50-iteration leak check (0 bytes leaked)
        ├── FullPipelineBenchmarkTest.java  # end-to-end: XML→Arrow→DuckDB→validate, types A–E
        ├── SampleGeneratorRunner.java      # standalone runner: generate sample files by type
        └── generator/
            ├── PainXmlGenerator.java       # generator interface
            ├── PainXmlGeneratorImpl.java   # StAX implementation
            ├── TestPainFileSpecs.java      # type constants A–E
            └── TestFileGenerator.java      # generate-if-absent + tail-check
```

---

## Running Tests

### Build and test all modules

```bash
export JAVA_HOME=/usr/lib/jvm/temurin-25-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# All tests (generates Type D and E automatically; A–C generated on demand)
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-validator --also-make
```

### Run a specific test

```bash
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn test -pl pgw-validator --also-make -Dtest=ArrowFileLoadBenchmarkTest

MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-validator --also-make -Dtest=FullPipelineBenchmarkTest
```

### Generate sample files manually

```bash
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn exec:java -pl pgw-validator \
    -Dexec.mainClass="com.pgw.SampleGeneratorRunner" \
    -Dexec.args="type-d type-e"
```

Sample files are written to `pgw-validator/src/test/resources/sample-data/`.

---

## Test Files

| Type | File | Remittances | Txns/Block | Total Txns | Purpose |
|------|------|-------------|------------|------------|---------|
| A | `pain001_type_a_1x1M.xml` | 1 | 1,000,000 | 1,000,000 | Fat batch benchmark |
| B | `pain001_type_b_2x500K.xml` | 2 | 500,000 | 1,000,000 | Multi-batch benchmark |
| C | `pain001_type_c_1Mx1.xml` | 1,000,000 | 1 | 1,000,000 | Adversarial (max overhead) |
| D | `pain001_type_d_2x100_valid.xml` | 2 | 100 | 200 | Valid — fast unit test |
| E | `pain001_type_e_2x100_invalid_ctrlsum.xml` | 2 | 100 | 200 | Invalid control sum — negative test |

> Types A–C (~516–888 MB) are not committed to the repo. They are generated on first test run.
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
diff -u $BASELINE $AFTER | grep -E "^[+-].*ms|Validation|Benchmark"
```

### Key Metrics to Watch

| Metric | Example |
|--------|---------|
| Parse throughput | `Parse Throughput : 127,730 rows/sec` |
| Validation time | `SQL Validation    :         65 ms` |
| Arrow off-heap peak | `Off-heap peak    :  25,640,960 bytes (24.5 MB)` |
| Total pod impact | `Total Pod Impact : 1,119.9 MB` |
| Memory leak | `0 bytes leaked` |

---

## Updating TEST_RESULTS.md

After running benchmarks, add a new section to [TEST_RESULTS.md](TEST_RESULTS.md):

```markdown
## Test Run: YYYY-MM-DD

### Changes
- Brief description

### Results

| File   | Parse ms | Val ms | Arrow MB | Heap ΔMB |
|--------|----------|--------|----------|----------|
| Type A | 9,236    | 60     | 224.4    | 12.6     |
| ...    |          |        |          |          |

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
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g" mvn test -pl pgw-validator --also-make
```

**Tests skip large file types (A–C)** — They generate on first run. Types A–C take ~14–23 seconds to generate and ~8–22 seconds to parse.

---

**Last Updated:** 2026-04-05
**Architecture:** Multi-module Maven (`pgw-ingestor` + `pgw-validator`)
**Package root:** `com.pgw`
