#!/bin/bash
#
# run_validation_tests.sh
# 
# Standardized test script for comparing validation framework performance
# Run this script before and after making changes to establish baselines
#

set -e

# Configuration
JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64
export JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH

# Output directory
TEST_OUTPUT_DIR="test-results"
mkdir -p "$TEST_OUTPUT_DIR"

# Timestamp for this test run
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="$TEST_OUTPUT_DIR/test_run_$TIMESTAMP.log"

echo "═══════════════════════════════════════════════════════════════"
echo "  ISO 20022 Validation Framework Test Suite"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Test Timestamp: $TIMESTAMP"
echo "Output File: $OUTPUT_FILE"
echo "Java Version:"
java -version 2>&1 | head -3
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Clean and compile
echo "Step 1: Clean and compile..."
mvn clean compile 2>&1 | tail -5

echo ""
echo "Step 2: Running test suite..."
echo ""

# Run the full test suite and capture output
mvn exec:java -Dexec.mainClass="com.iso20022.pain.App" 2>&1 | tee "$OUTPUT_FILE"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Test Results Summary"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Extract and display validation times
echo "Validation Times:"
echo "─────────────────"
grep "Validation.*ms" "$OUTPUT_FILE" | sed 's/.*║/  -/' || echo "  (No validation times found)"

echo ""
echo "Validation Execution Mode:"
echo "──────────────────────────"
grep "Executing.*validator" "$OUTPUT_FILE" | sed 's/.*INFO.*- /  - /' || echo "  (No execution mode found)"

echo ""
echo "Validation Results:"
echo "───────────────────"
grep "All validations passed\|Validation failed" "$OUTPUT_FILE" | sed 's/.*INFO.*- /  - /' || echo "  (No validation results found)"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "✓ Test complete! Full output saved to: $OUTPUT_FILE"
echo ""
echo "To compare with previous run:"
echo "  diff -u $TEST_OUTPUT_DIR/test_run_PREVIOUS.log $OUTPUT_FILE"
echo ""
echo "To extract benchmark data:"
echo "  grep -A 30 'BENCHMARK:' $OUTPUT_FILE"
echo ""
echo "═══════════════════════════════════════════════════════════════"
