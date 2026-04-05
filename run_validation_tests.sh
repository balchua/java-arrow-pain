#!/bin/bash
#
# run_validation_tests.sh
#
# Standardized test script for running and comparing validation framework
# performance across code changes.
#
# Usage:
#   ./run_validation_tests.sh
#
# Results are saved to test-results/test_run_YYYYMMDD_HHMMSS.log

set -e

# ── Java 25 is required for virtual threads + records ─────────────────────────
JAVA_HOME=/usr/lib/jvm/temurin-25-jdk-amd64
export JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH

# ── Output directory ──────────────────────────────────────────────────────────
TEST_OUTPUT_DIR="test-results"
mkdir -p "$TEST_OUTPUT_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="$TEST_OUTPUT_DIR/test_run_$TIMESTAMP.log"

echo "═══════════════════════════════════════════════════════════════"
echo "  PGW — ISO 20022 pain.001 Validation Test Suite"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "  Timestamp  : $TIMESTAMP"
echo "  Output     : $OUTPUT_FILE"
echo "  Java       : $(java -version 2>&1 | head -1)"
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo ""

# ── Step 1: compile ───────────────────────────────────────────────────────────
echo "Step 1: Clean and compile all modules..."
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn clean compile -pl pgw-ingestor,pgw-validator --also-make 2>&1 | tail -5

echo ""
echo "Step 2: Running full test suite (pgw-validator)..."
echo ""

# ── Step 2: run tests, tee output ─────────────────────────────────────────────
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
  mvn test -pl pgw-validator --also-make 2>&1 | tee "$OUTPUT_FILE"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Summary"
echo "═══════════════════════════════════════════════════════════════"
echo ""

echo "Validation Times:"
grep "Validation completed in" "$OUTPUT_FILE" | \
  sort -u | sed 's/.*INFO.*- /  /' || echo "  (none found)"

echo ""
echo "Test Results:"
grep "Tests run:" "$OUTPUT_FILE" | sed 's/\[INFO\] /  /' || echo "  (none found)"

echo ""
echo "Build:"
grep "BUILD " "$OUTPUT_FILE" | tail -1 | sed 's/\[INFO\] /  /'

echo ""
echo "✓ Full output saved to: $OUTPUT_FILE"
echo ""
echo "Compare with a previous run:"
echo "  diff -u $TEST_OUTPUT_DIR/test_run_<PREV>.log $OUTPUT_FILE"
echo "Extract benchmark tables:"
echo "  grep -A 20 'BENCHMARK:' $OUTPUT_FILE"
echo ""
echo "═══════════════════════════════════════════════════════════════"
