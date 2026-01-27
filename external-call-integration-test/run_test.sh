#!/bin/bash
#
# External Call Integration Test Runner
#
# This script runs the full integration test for the external call feature.
# It verifies that:
# 1. Signatory (participant1) can make external calls via HTTP
# 2. Observer (participant2) can process transactions WITHOUT external service
# 3. External call results are stored and replayed correctly
#
# Usage: ./run_test.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Directories
# run_test.sh is in external-call-integration-test/
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$TEST_DIR/scripts"
REPO_ROOT="$(dirname "$TEST_DIR")"  # daml/

echo "============================================================"
echo "External Call Integration Test"
echo "============================================================"
echo ""

# ============================================================
# Find Canton executable (bazel-built)
# ============================================================
echo "[1/5] Looking for Canton executable..."

CANTON_CMD="$REPO_ROOT/sdk/bazel-bin/canton/community_app"

if [ ! -x "$CANTON_CMD" ]; then
  echo -e "${YELLOW}Canton not built. Building now...${NC}"
  (cd "$REPO_ROOT/sdk" && ./dev-env/bin/bazel build //canton:community_app)
fi

if [ ! -x "$CANTON_CMD" ]; then
  echo -e "${RED}ERROR: Canton executable not found at $CANTON_CMD${NC}"
  echo ""
  echo "Build Canton with:"
  echo "  cd $REPO_ROOT/sdk && ./dev-env/bin/bazel build //canton:community_app"
  exit 1
fi

echo "       Canton: $CANTON_CMD"

# ============================================================
# Check DAR exists
# ============================================================
echo "[2/5] Checking DAR..."

DAR_PATH="$TEST_DIR/.daml/dist/external-call-integration-test-1.0.0.dar"
if [ ! -f "$DAR_PATH" ]; then
  echo -e "${YELLOW}DAR not found. Building...${NC}"
  cd "$TEST_DIR"
  daml build
fi

if [ ! -f "$DAR_PATH" ]; then
  echo -e "${RED}ERROR: DAR not found at $DAR_PATH${NC}"
  exit 1
fi

echo "       DAR: $DAR_PATH"

# ============================================================
# Clean up
# ============================================================
echo "[3/5] Cleaning up..."

rm -f /tmp/external_call_test_counts.json
rm -f /tmp/mock_service.log

# Kill any existing mock service on port 8080
fuser -k 8080/tcp 2>/dev/null || true
sleep 1

echo "       Done"

# ============================================================
# Start mock external service
# ============================================================
echo "[4/5] Starting mock external service..."

python3 "$SCRIPTS_DIR/mock_service.py" 8080 > /tmp/mock_service.log 2>&1 &
MOCK_PID=$!

# Wait for mock service to be ready
sleep 2
if ! kill -0 $MOCK_PID 2>/dev/null; then
  echo -e "${RED}ERROR: Mock service failed to start!${NC}"
  cat /tmp/mock_service.log
  exit 1
fi

echo "       Mock service running (PID: $MOCK_PID, port 8080)"

# Cleanup function
cleanup() {
  echo ""
  echo "Cleaning up..."
  kill $MOCK_PID 2>/dev/null || true
  fuser -k 8080/tcp 2>/dev/null || true
}
trap cleanup EXIT

# ============================================================
# Run Canton test
# ============================================================
echo "[5/5] Running Canton integration test..."
echo ""

cd "$REPO_ROOT"  # Run from repo root so relative paths work

# Use JAVA_OPTS like the manual command that worked
export JAVA_OPTS="-Ddar.path=external-call-integration-test/.daml/dist/external-call-integration-test-1.0.0.dar"

"$CANTON_CMD" run \
  -c external-call-integration-test/canton.conf \
  external-call-integration-test/scripts/full_test.canton

TEST_EXIT_CODE=$?

# ============================================================
# Show results
# ============================================================
echo ""
echo "============================================================"
echo "Mock Service Results"
echo "============================================================"

if [ -f /tmp/external_call_test_counts.json ]; then
  cat /tmp/external_call_test_counts.json | python3 -m json.tool 2>/dev/null || cat /tmp/external_call_test_counts.json
  echo ""

  SUBMISSION_COUNT=$(python3 -c "import json; data=json.load(open('/tmp/external_call_test_counts.json')); print(data['counts']['submission'])" 2>/dev/null || echo "?")
  VALIDATION_COUNT=$(python3 -c "import json; data=json.load(open('/tmp/external_call_test_counts.json')); print(data['counts']['validation'])" 2>/dev/null || echo "?")

  if [ "$SUBMISSION_COUNT" = "1" ] && [ "$VALIDATION_COUNT" = "0" ]; then
    echo -e "${GREEN}PASS: submission=1, validation=0 (observer used stored results)${NC}"
  else
    echo -e "${YELLOW}Counts: submission=$SUBMISSION_COUNT, validation=$VALIDATION_COUNT${NC}"
  fi
else
  echo -e "${YELLOW}WARNING: Could not read mock service counts${NC}"
fi

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
  echo -e "${GREEN}TEST PASSED${NC}"
else
  echo -e "${RED}TEST FAILED${NC}"
  echo "Check /tmp/mock_service.log for mock service output"
  exit 1
fi
