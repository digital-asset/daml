# External Call Integration Tests

Integration tests for the external call feature. Run with `./run_test.sh --all`.

## Test Coverage

### Happy Path
- [x] Basic external call with response (`full_test.canton`)
- [x] Observer replay without HTTP call - verifies stored results work
- [x] Multiple calls in single choice (`test_edge_cases.canton`)
- [x] Different function IDs

### HTTP Errors (`test_errors.canton`)
All error codes tested: 400, 401, 403, 404, 500, 502, 503, 504

### Timeouts (`test_timeout.canton`)
- [x] Request timeout (delay > timeout)
- [ ] Connection timeout (needs network-level mock)
- [ ] Max total timeout

### Retry Logic (`test_retry.canton`)
- [x] Retry on 503 then success
- [x] Max retries exceeded (always 503)
- [x] Rate limit 429
- [ ] Retry-After header timing
- [ ] Exponential backoff verification

### JWT Auth (`test_auth.canton`)
- [x] Valid token sent and accepted
- [x] Header format verification (jwt-echo)
- [x] Server rejects token (401)
- [x] Missing token (`test_edge_cases.canton`)
- [ ] Token from file

### TLS (`test_tls.canton`)
- [x] HTTPS with self-signed cert (insecure mode)
- [x] TLS connection works
- [ ] Cert validation failure
- [ ] HTTP/HTTPS mismatch

### Input/Output Edge Cases (`test_edge_cases.canton`)
- [x] Empty input rejected
- [x] Large input (10KB)
- [x] Large output (100KB)
- [x] Unicode bytes (hex-encoded)
- [x] All byte values 00-FF
- [x] Invalid hex rejected

### Config Edge Cases (`test_config.canton`)
- [x] Unknown extension ID
- [x] Unknown function ID (allowed by default)
- [x] Config hash mismatch (allowed by default)
- [ ] Startup validation (needs different test approach)

### Multi-Participant (`test_multi_*.canton`)
- [x] Signatory has extension, observer doesn't (covered by happy path)
- [x] Both participants have extension
- [x] Neither has extension - proper error

### Echo Mode (`test_echo.canton`)
- [x] Returns input as output, no HTTP calls

## Running

```bash
./run_test.sh              # happy path only
./run_test.sh --all        # all 39 tests
./run_test.sh --errors     # just error handling
./run_test.sh --retry      # just retry logic
./run_test.sh --auth       # JWT tests
./run_test.sh --tls        # TLS tests
./run_test.sh --edge       # edge cases
./run_test.sh --multi      # multi-participant
./run_test.sh --config     # config edge cases
./run_test.sh --echo       # echo mode
./run_test.sh --rebuild-dar --all  # rebuild DAR first
```

## Files

```
canton.conf                  # main config (participant1 has extension, participant2 doesn't)
canton-auth.conf             # adds JWT token
canton-tls.conf              # TLS enabled
canton-echo.conf             # echo mode
canton-both-extensions.conf  # both participants have extension
canton-no-extensions.conf    # neither has extension

scripts/
  mock_service.py           # configurable mock (errors, delays, JWT, TLS)
  full_test.canton          # happy path
  test_errors.canton        # HTTP error codes
  test_retry.canton         # retry logic
  test_auth.canton          # JWT
  test_tls.canton           # TLS
  test_timeout.canton       # timeouts
  test_edge_cases.canton    # input/output edge cases
  test_config.canton        # config edge cases
  test_multi_both.canton    # both have extension
  test_multi_none.canton    # neither has extension
  test_echo.canton          # echo mode
  generate_certs.sh         # creates self-signed certs for TLS tests
```

## Mock Service

The mock (`scripts/mock_service.py`) responds based on function_id:
- `echo` - returns input
- `error-{code}` - returns that HTTP status
- `delay-{ms}` - delays response
- `retry-once` - 503 first, then 200
- `retry-always` - always 503
- `rate-limit` - 429 with Retry-After
- `jwt-required` - checks for Bearer token
- `jwt-echo` - echoes auth header
- `jwt-invalid` - always 401
- `large-output` - returns 100KB
- `tls-test` - for TLS verification

## Not Implemented

These are tricky to test reliably:
- Retry-After timing verification
- Exponential backoff timing
- Startup validation (needs Canton restart mid-test)
- Connection-level timeouts (need network simulation)
