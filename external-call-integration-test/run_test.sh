#!/bin/bash
#
# External Call Integration Test Runner
#
# Usage:
#   ./run_test.sh              # Run happy path tests (default)
#   ./run_test.sh --all        # Run all 39 tests
#   ./run_test.sh --errors     # Run error handling tests (8 tests)
#   ./run_test.sh --retry      # Run retry logic tests (3 tests)
#   ./run_test.sh --auth       # Run JWT authentication tests (3 tests)
#   ./run_test.sh --tls        # Run TLS tests (2 tests)
#   ./run_test.sh --timeout    # Run timeout tests (2 tests)
#   ./run_test.sh --edge       # Run edge case tests (9 tests)
#   ./run_test.sh --echo       # Run echo mode tests (3 tests)
#   ./run_test.sh --multi      # Run multi-participant tests (4 tests)
#   ./run_test.sh --config     # Run config edge case tests (3 tests)
#   ./run_test.sh --help       # Show help
#
# The script:
# 1. Finds the bazel-built Canton
# 2. Checks/builds the DAR
# 3. Starts the mock service
# 4. Runs the Canton integration test(s)
# 5. Shows results and cleans up

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Directories
# run_test.sh is in external-call-integration-test/
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$TEST_DIR/scripts"
REPO_ROOT="$(dirname "$TEST_DIR")"  # daml/

# Parse arguments
TEST_SUITE="happy"  # default
REBUILD_DAR=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --all)
      TEST_SUITE="all"
      shift
      ;;
    --errors)
      TEST_SUITE="errors"
      shift
      ;;
    --retry)
      TEST_SUITE="retry"
      shift
      ;;
    --auth)
      TEST_SUITE="auth"
      shift
      ;;
    --tls)
      TEST_SUITE="tls"
      shift
      ;;
    --timeout)
      TEST_SUITE="timeout"
      shift
      ;;
    --edge)
      TEST_SUITE="edge"
      shift
      ;;
    --echo)
      TEST_SUITE="echo"
      shift
      ;;
    --multi)
      TEST_SUITE="multi"
      shift
      ;;
    --config)
      TEST_SUITE="config"
      shift
      ;;
    --happy)
      TEST_SUITE="happy"
      shift
      ;;
    --rebuild-dar)
      REBUILD_DAR=true
      shift
      ;;
    --help|-h)
      echo "External Call Integration Test Runner"
      echo ""
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --happy       Run happy path tests only (default)"
      echo "  --errors      Run error handling tests"
      echo "  --retry       Run retry logic tests"
      echo "  --auth        Run JWT authentication tests"
      echo "  --tls         Run TLS tests (requires certificate generation)"
      echo "  --timeout     Run timeout tests"
      echo "  --edge        Run edge case tests (empty input, large input, etc.)"
      echo "  --echo        Run echo mode tests (no mock service needed)"
      echo "  --multi       Run multi-participant tests"
      echo "  --config      Run config edge case tests"
      echo "  --all         Run all tests"
      echo "  --rebuild-dar Force rebuild of DAR file"
      echo "  --help        Show this help"
      echo ""
      echo "Test suites:"
      echo "  happy  - Basic external call functionality, observer replay"
      echo "  errors - HTTP error codes (400, 401, 403, 404, 500, 503)"
      echo "  retry  - Retry logic (retry-once, max-retries, rate-limit)"
      echo "  auth   - JWT authentication (valid token, server rejection)"
      echo "  tls    - TLS connectivity with self-signed certificates"
      echo "  timeout - Request and max timeout handling"
      echo "  edge   - Edge cases (empty input, large input, missing JWT, etc.)"
      echo "  echo   - Echo mode (no HTTP calls, returns input as output)"
      echo "  multi  - Multi-participant scenarios (both/neither have extension)"
      echo "  config - Config edge cases (unknown extension, unknown function, hash mismatch)"
      echo ""
      echo "Local SDK:"
      echo "  To install the local Daml SDK with your changes:"
      echo "  $SCRIPTS_DIR/install_local_sdk.sh"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

echo "============================================================"
echo "External Call Integration Test"
echo "============================================================"
echo ""
echo -e "Test Suite: ${BLUE}$TEST_SUITE${NC}"
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

# Force rebuild if requested
if [ "$REBUILD_DAR" = true ] && [ -f "$DAR_PATH" ]; then
  echo -e "${YELLOW}Removing old DAR for rebuild...${NC}"
  rm -f "$DAR_PATH"
fi

if [ ! -f "$DAR_PATH" ]; then
  echo -e "${YELLOW}DAR not found. Building...${NC}"

  # Try to use local damlc from bazel if available (preferred - has latest changes)
  LOCAL_DAMLC="$REPO_ROOT/sdk/bazel-bin/compiler/damlc/damlc"
  if [ -x "$LOCAL_DAMLC" ]; then
    echo "       Using local damlc from bazel build"
    cd "$TEST_DIR"
    mkdir -p .daml/dist
    "$LOCAL_DAMLC" build --project-root . -o .daml/dist/external-call-integration-test-1.0.0.dar
  elif command -v daml &> /dev/null; then
    echo "       Using installed daml"
    cd "$TEST_DIR"
    daml build
  else
    echo -e "${RED}ERROR: Neither local damlc nor installed daml found${NC}"
    echo ""
    echo "Options:"
    echo "  1. Build local damlc: cd $REPO_ROOT/sdk && ./dev-env/bin/bazel build //compiler/damlc:damlc"
    echo "  2. Install local SDK: $SCRIPTS_DIR/install_local_sdk.sh"
    exit 1
  fi
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

# Kill any existing mock service on port 8080 or 8443
fuser -k 8080/tcp 2>/dev/null || true
fuser -k 8443/tcp 2>/dev/null || true
sleep 1

echo "       Done"

# ============================================================
# TLS Certificate Setup (if needed)
# ============================================================
if [ "$TEST_SUITE" = "tls" ]; then
  echo "[3.5/5] Checking TLS certificates..."
  CERT_DIR="$TEST_DIR/certs"

  if [ ! -f "$CERT_DIR/server.crt" ] || [ ! -f "$CERT_DIR/server.key" ]; then
    echo -e "${YELLOW}Generating self-signed certificates...${NC}"
    "$SCRIPTS_DIR/generate_certs.sh"
  fi

  if [ ! -f "$CERT_DIR/server.crt" ]; then
    echo -e "${RED}ERROR: TLS certificates not found!${NC}"
    echo "Run: $SCRIPTS_DIR/generate_certs.sh"
    exit 1
  fi

  echo "       Certificates: $CERT_DIR/server.crt"
fi

# ============================================================
# Start mock external service
# ============================================================
echo "[4/5] Starting mock external service..."

if [ "$TEST_SUITE" = "tls" ]; then
  # TLS tests: Start mock with TLS on port 8443
  CERT_DIR="$TEST_DIR/certs"
  python3 "$SCRIPTS_DIR/mock_service.py" 8443 --tls --cert "$CERT_DIR/server.crt" --key "$CERT_DIR/server.key" > /tmp/mock_service.log 2>&1 &
  MOCK_PID=$!
  MOCK_PORT=8443
  echo "       Mock service (TLS) running (PID: $MOCK_PID, port $MOCK_PORT)"
else
  # Non-TLS tests: Start mock on port 8080
  python3 "$SCRIPTS_DIR/mock_service.py" 8080 > /tmp/mock_service.log 2>&1 &
  MOCK_PID=$!
  MOCK_PORT=8080
  echo "       Mock service running (PID: $MOCK_PID, port $MOCK_PORT)"
fi

# Wait for mock service to be ready
sleep 2
if ! kill -0 $MOCK_PID 2>/dev/null; then
  echo -e "${RED}ERROR: Mock service failed to start!${NC}"
  cat /tmp/mock_service.log
  exit 1
fi

# Cleanup function
cleanup() {
  echo ""
  echo "Cleaning up..."
  kill $MOCK_PID 2>/dev/null || true
  fuser -k 8080/tcp 2>/dev/null || true
  fuser -k 8443/tcp 2>/dev/null || true
}
trap cleanup EXIT

# ============================================================
# Run Canton test(s)
# ============================================================
echo "[5/5] Running Canton integration test..."
echo ""

cd "$REPO_ROOT"  # Run from repo root so relative paths work

# Use JAVA_OPTS like the manual command that worked
export JAVA_OPTS="-Ddar.path=external-call-integration-test/.daml/dist/external-call-integration-test-1.0.0.dar"

run_test() {
  local test_name=$1
  local script_name=$2
  local config_file=${3:-"canton.conf"}  # Default to canton.conf

  echo ""
  echo -e "${BLUE}Running: $test_name${NC}"
  echo "Script: $script_name"
  echo "Config: $config_file"
  echo ""

  "$CANTON_CMD" run \
    -c "external-call-integration-test/$config_file" \
    "external-call-integration-test/scripts/$script_name"

  return $?
}

TEST_EXIT_CODE=0

case $TEST_SUITE in
  happy)
    run_test "Happy Path Tests" "full_test.canton" || TEST_EXIT_CODE=1
    ;;
  errors)
    run_test "Error Handling Tests" "test_errors.canton" || TEST_EXIT_CODE=1
    ;;
  retry)
    run_test "Retry Logic Tests" "test_retry.canton" || TEST_EXIT_CODE=1
    ;;
  auth)
    # Auth tests use canton-auth.conf which has JWT configured
    run_test "JWT Authentication Tests" "test_auth.canton" "canton-auth.conf" || TEST_EXIT_CODE=1
    ;;
  tls)
    # TLS tests use canton-tls.conf which has use-tls = true
    run_test "TLS Tests" "test_tls.canton" "canton-tls.conf" || TEST_EXIT_CODE=1
    ;;
  timeout)
    run_test "Timeout Tests" "test_timeout.canton" || TEST_EXIT_CODE=1
    ;;
  edge)
    run_test "Edge Case Tests" "test_edge_cases.canton" || TEST_EXIT_CODE=1
    ;;
  echo)
    # Echo mode tests don't need mock service - kill it
    kill $MOCK_PID 2>/dev/null || true
    run_test "Echo Mode Tests" "test_echo.canton" "canton-echo.conf" || TEST_EXIT_CODE=1
    ;;
  multi)
    # Multi-participant tests
    echo -e "${BLUE}Running multi-participant tests...${NC}"
    echo ""

    # T9.2: Both participants have extension
    run_test "Multi-Participant (Both Have Extension)" "test_multi_both.canton" "canton-both-extensions.conf" || TEST_EXIT_CODE=1
    echo ""
    echo "============================================================"
    echo ""

    # Reset mock between tests
    curl -s http://127.0.0.1:8080/reset > /dev/null 2>&1 || true

    # T9.3: Neither participant has extension (no mock needed but doesn't hurt)
    run_test "Multi-Participant (Neither Has Extension)" "test_multi_none.canton" "canton-no-extensions.conf" || TEST_EXIT_CODE=1
    ;;
  config)
    # Config edge case tests
    run_test "Config Edge Case Tests" "test_config.canton" || TEST_EXIT_CODE=1
    ;;
  all)
    echo -e "${BLUE}Running all test suites...${NC}"
    echo ""

    # === HTTP-based tests (mock on port 8080) ===
    run_test "Happy Path Tests" "full_test.canton" || TEST_EXIT_CODE=1
    echo ""
    echo "============================================================"
    echo ""

    # Reset mock between test suites
    curl -s http://127.0.0.1:8080/reset > /dev/null 2>&1 || true

    run_test "Error Handling Tests" "test_errors.canton" || TEST_EXIT_CODE=1
    echo ""
    echo "============================================================"
    echo ""

    # Reset mock between test suites
    curl -s http://127.0.0.1:8080/reset > /dev/null 2>&1 || true

    run_test "Retry Logic Tests" "test_retry.canton" || TEST_EXIT_CODE=1
    echo ""
    echo "============================================================"
    echo ""

    # Reset mock between test suites
    curl -s http://127.0.0.1:8080/reset > /dev/null 2>&1 || true

    # Auth tests use different config
    run_test "JWT Authentication Tests" "test_auth.canton" "canton-auth.conf" || TEST_EXIT_CODE=1
    echo ""
    echo "============================================================"
    echo ""

    # Reset mock between test suites
    curl -s http://127.0.0.1:8080/reset > /dev/null 2>&1 || true

    run_test "Timeout Tests" "test_timeout.canton" || TEST_EXIT_CODE=1
    echo ""
    echo "============================================================"
    echo ""

    # Reset mock between test suites
    curl -s http://127.0.0.1:8080/reset > /dev/null 2>&1 || true

    run_test "Edge Case Tests" "test_edge_cases.canton" || TEST_EXIT_CODE=1
    echo ""
    echo "============================================================"
    echo ""

    # === TLS tests (need mock on port 8443 with TLS) ===
    echo -e "${BLUE}Switching to TLS mock service for TLS tests...${NC}"

    # Stop HTTP mock
    kill $MOCK_PID 2>/dev/null || true
    fuser -k 8080/tcp 2>/dev/null || true
    sleep 1

    # Check/generate TLS certificates
    CERT_DIR="$TEST_DIR/certs"
    if [ ! -f "$CERT_DIR/server.crt" ] || [ ! -f "$CERT_DIR/server.key" ]; then
      echo -e "${YELLOW}Generating self-signed certificates...${NC}"
      "$SCRIPTS_DIR/generate_certs.sh"
    fi

    # Start TLS mock
    python3 "$SCRIPTS_DIR/mock_service.py" 8443 --tls --cert "$CERT_DIR/server.crt" --key "$CERT_DIR/server.key" > /tmp/mock_service.log 2>&1 &
    MOCK_PID=$!
    sleep 2

    if ! kill -0 $MOCK_PID 2>/dev/null; then
      echo -e "${RED}ERROR: TLS mock service failed to start!${NC}"
      cat /tmp/mock_service.log
      TEST_EXIT_CODE=1
    else
      echo "       TLS mock service running (PID: $MOCK_PID, port 8443)"
      run_test "TLS Tests" "test_tls.canton" "canton-tls.conf" || TEST_EXIT_CODE=1
    fi
    echo ""
    echo "============================================================"
    echo ""

    # === Echo mode tests (no mock service needed) ===
    echo -e "${BLUE}Running echo mode tests (no mock service needed)...${NC}"
    kill $MOCK_PID 2>/dev/null || true
    fuser -k 8443/tcp 2>/dev/null || true

    run_test "Echo Mode Tests" "test_echo.canton" "canton-echo.conf" || TEST_EXIT_CODE=1
    echo ""
    echo "============================================================"
    echo ""

    # === Multi-participant tests ===
    echo -e "${BLUE}Running multi-participant tests...${NC}"

    # Start mock service again for multi tests
    python3 "$SCRIPTS_DIR/mock_service.py" 8080 > /tmp/mock_service.log 2>&1 &
    MOCK_PID=$!
    sleep 2

    # T9.2: Both participants have extension
    run_test "Multi-Participant (Both Have Extension)" "test_multi_both.canton" "canton-both-extensions.conf" || TEST_EXIT_CODE=1
    echo ""
    echo "============================================================"
    echo ""

    # Reset mock between tests
    curl -s http://127.0.0.1:8080/reset > /dev/null 2>&1 || true

    # T9.3: Neither participant has extension (no mock needed but doesn't hurt)
    run_test "Multi-Participant (Neither Has Extension)" "test_multi_none.canton" "canton-no-extensions.conf" || TEST_EXIT_CODE=1
    echo ""
    echo "============================================================"
    echo ""

    # === Config edge case tests ===
    echo -e "${BLUE}Running config edge case tests...${NC}"

    # Reset mock between tests
    curl -s http://127.0.0.1:8080/reset > /dev/null 2>&1 || true

    run_test "Config Edge Case Tests" "test_config.canton" || TEST_EXIT_CODE=1
    ;;
esac

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
