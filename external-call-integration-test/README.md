# External Call Integration Test

Tests for the external call feature, specifically verifying that observers can replay transactions using stored results without needing access to the external service.

## Quick Start

```bash
./run_test.sh --all    # run all tests
./run_test.sh          # just happy path
./run_test.sh --help   # see all options
```

## What's Being Tested

The key property: participant2 (observer) has NO extension configured, yet it can process transactions with external calls because results are stored in the transaction.

```
participant1 (signatory)     participant2 (observer)
  - has extension              - NO extension
  - makes HTTP calls           - uses stored results
```

## Test Suites

| Flag | Tests |
|------|-------|
| `--happy` | Basic call + observer replay |
| `--errors` | HTTP error codes (400-504) |
| `--retry` | Retry logic |
| `--auth` | JWT authentication |
| `--tls` | TLS/HTTPS |
| `--timeout` | Request timeouts |
| `--edge` | Input/output edge cases |
| `--multi` | Multi-participant scenarios |
| `--config` | Config edge cases |
| `--echo` | Echo mode (no HTTP) |

## Manual Testing

```bash
# Terminal 1: mock service
python3 scripts/mock_service.py 8080

# Terminal 2: Canton
export JAVA_OPTS="-Ddar.path=.daml/dist/external-call-integration-test-1.0.0.dar"
../../sdk/bazel-bin/canton/community_app run -c canton.conf scripts/full_test.canton
```

## Mock Service

Responds based on function_id: `echo`, `error-{code}`, `delay-{ms}`, `retry-once`, etc.
Logs to stdout and writes stats to `/tmp/external_call_test_counts.json`.

## Config Files

- `canton.conf` - participant1 has extension, participant2 doesn't
- `canton-auth.conf` - includes JWT token
- `canton-tls.conf` - TLS enabled
- `canton-echo.conf` - echo mode (no HTTP calls)
- `canton-both-extensions.conf` - both participants have extension
- `canton-no-extensions.conf` - neither has extension

See [TEST_PLAN.md](TEST_PLAN.md) for full test coverage.
