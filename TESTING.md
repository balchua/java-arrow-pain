# Testing Guide

This document describes the testing infrastructure for comparing validation framework performance across code changes.

## Quick Start

```bash
# Run the standardized test suite
./run_validation_tests.sh
```

Results are saved to `test-results/test_run_YYYYMMDD_HHMMSS.log`

## Test Infrastructure

### Files

| File | Purpose |
|------|---------|
| `run_validation_tests.sh` | Automated test execution script |
| `TEST_RESULTS.md` | Current baseline test results and comparison |
| `test-results/` | Directory for test output logs (gitignored) |
| `TESTING.md` | This file - testing documentation |

### What Gets Tested

The test suite:
1. Compiles the project (`mvn clean compile`)
2. Generates test XML files (Type A, B, C)
3. Parses XML into Arrow format using streaming pipeline
4. Runs validation pipeline with all validators
5. Streams Arrow IPC batches via PersistenceService (local or S3)
6. Captures complete benchmark results

### Configuration

See the [Configuration](README.md#configuration) section in README.md for the environment variables
that control the persistence mode and output directory:

| Env var | Default | Description |
|---|---|---|
| `PAIN_PERSISTENCE_MODE` | `local` | `local` or `s3` |
| `PAIN_LOCAL_OUTPUT_DIR` | `src/main/resources/output` | Local output directory for Arrow IPC Stream files |
| `PAIN_S3_BUCKET` | _(required for s3)_ | Target S3 bucket name |
| `PAIN_S3_KEY_PREFIX` | `pain001` | S3 key prefix (folder) |

### Test Files

| File | Structure | Rows | Purpose |
|------|-----------|------|---------|
| Type A | 1×1M | 1M transactions, 1 remittance | Fat batch scenario |
| Type B | 2×500K | 1M transactions, 2 remittances | Multiple batches |
| Type C | 1M×1 | 1M transactions, 1M remittances | Adversarial (max overhead) |

## Comparing Changes

### Before/After Workflow

```bash
# 1. Establish baseline BEFORE changes
./run_validation_tests.sh
BASELINE=test-results/test_run_$(date +%Y%m%d_%H%M%S).log

# 2. Make your code changes
# ... edit files ...

# 3. Run tests AFTER changes
./run_validation_tests.sh
AFTER=test-results/test_run_$(date +%Y%m%d_%H%M%S).log

# 4. Compare results
diff -u $BASELINE $AFTER | less

# Or extract specific metrics
echo "=== Baseline ==="
grep "Validation.*ms" $BASELINE
echo "=== After Changes ==="
grep "Validation.*ms" $AFTER
```

### What to Compare

#### Key Metrics

1. **Validation Time** (primary metric)
   ```
   Validation        :        223 ms  (0.22 s)
   ```

2. **Execution Mode**
   ```
   INFO ValidationPipeline - Executing 4 validator(s) in PARALLEL mode (virtual threads)
   ```

3. **Memory Usage**
   ```
   Combined peak    :     361,763,392 bytes (345.0 MB)
   ```

4. **Parse Throughput**
   ```
   Parse Throughput : 189,897 rows/sec
   Parse Throughput : 73.56 MB/sec
   ```

5. **Validation Results**
   ```
   ✓ All validations passed (4 validators, 223 ms)
   ```

### Example Analysis

```
Metric: Validation Time (Type A)
Before: 154 ms
After:  223 ms
Diff:   +69 ms (+45%)

Explanation:
- Added 3 new validators (Message, Remittance, Transaction)
- Each validator scans 1M transaction rows
- Parallel execution reduces overhead
- Trade-off: +69ms for comprehensive validation

Verdict: Acceptable - 69ms is <2% of total 5.6s pipeline
```

## Updating Documentation

After running comparison tests, update `TEST_RESULTS.md`:

### Template for New Section

```markdown
## Test Run: YYYY-MM-DD

### Changes
- Brief description of what changed

### Results

| File   | Previous | Current | Diff | Analysis |
|--------|----------|---------|------|----------|
| Type A | 223 ms   | 215 ms  | -8ms | Optimization X improved performance |
| Type B | 172 ms   | 178 ms  | +6ms | Within noise margin |
| Type C | 864 ms   | 850 ms  | -14ms| Benefit scales with data size |

### Conclusions
- Summary of findings
- Any behavioral changes
- Recommendations
```

## Continuous Integration

### CI/CD Integration (Future)

For automated regression detection:

```yaml
# .github/workflows/validation-tests.yml
name: Validation Performance Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-java@v2
        with:
          java-version: '21'
      - name: Run validation tests
        run: ./run_validation_tests.sh
      - name: Archive test results
        uses: actions/upload-artifact@v2
        with:
          name: test-results
          path: test-results/
```

## Best Practices

### Do's

✅ Run tests on same hardware for fair comparison  
✅ Run multiple times if results vary (system load)  
✅ Document environment (Java version, system specs)  
✅ Explain performance changes in TEST_RESULTS.md  
✅ Keep test logs for at least 2-3 major changes  

### Don'ts

❌ Don't compare tests from different machines  
❌ Don't ignore small but consistent differences  
❌ Don't make changes without establishing baseline  
❌ Don't commit test logs to git (gitignored)  
❌ Don't skip documenting performance regressions  

## Troubleshooting

### Tests Fail to Run

```bash
# Ensure Java 21 is installed
java -version

# Should show: openjdk version "21.x.x"
# If not, install Java 21 or update JAVA_HOME in script
```

### Performance Varies Between Runs

- System load affects results
- Run 3 times, take median
- Look for consistent patterns, not absolute numbers
- Focus on relative changes (%, not ms)

### Memory Errors

```bash
# If you see OutOfMemoryError, increase heap:
MAVEN_OPTS="-Xmx4g" ./run_validation_tests.sh
```

## Reference

- Baseline results: [TEST_RESULTS.md](TEST_RESULTS.md)
- Architecture: [README.md#validation-framework](README.md#validation-framework)
- Code: `src/main/java/com/iso20022/pain/validation/`

---

**Last Updated:** 2026-02-15  
**Maintainer:** Development Team
