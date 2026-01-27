# External Call Integration Test

This directory contains an integration test that verifies the external call feature works correctly with stored result replay.

## What This Test Proves

The test demonstrates that:

1. **Signatory (participant1)** can make external HTTP calls during choice execution
2. **External call results are stored** in the transaction
3. **Observer (participant2)** can process transactions **without** needing the external service
4. The observer uses **stored results** instead of making HTTP calls

This is the key property of the external call feature: observers and validators can replay transactions deterministically using the stored results, without needing access to the external service.

## Test Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        TEST SETUP                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │   Participant1   │         │   Participant2   │              │
│  │   (SIGNATORY)    │         │   (OBSERVER)     │              │
│  │                  │         │                  │              │
│  │  - Hosts Alice   │         │  - Hosts Bob     │              │
│  │  - HAS extension │         │  - NO extension  │              │
│  │    configured    │         │    configured!   │              │
│  └────────┬─────────┘         └──────────────────┘              │
│           │                                                     │
│           │ HTTP call (mode=submission)                         │
│           ▼                                                     │
│  ┌──────────────────┐                                           │
│  │  Mock Extension  │                                           │
│  │  Service (:8080) │                                           │
│  └──────────────────┘                                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

The KEY INSIGHT: Participant2 has NO extension service configured,
yet it can still process transactions containing external calls
because it uses the STORED RESULTS from the transaction.
```

## Directory Structure

```
external-call-integration-test/
├── README.md                    # This file
├── daml.yaml                    # Daml project config
├── canton.conf                  # Canton configuration
├── run_test.sh                  # Master test runner script
├── daml/
│   └── ExternalCallIntegrationTest.daml  # Test contract
└── scripts/
    ├── mock_service.py          # Mock extension service
    ├── bootstrap.canton         # Canton bootstrap script
    └── run_test.canton          # Test execution script
```

## Prerequisites

1. **Canton** - Built and available (see below)
2. **Daml SDK** - For building the DAR
3. **Python 3** - For the mock service

## Running the Test

### Option 1: Using the test runner script

```bash
# If Canton is in PATH or CANTON_HOME is set
./run_test.sh

# Or specify Canton location
./run_test.sh --canton-home /path/to/canton
```

### Option 2: Manual execution

```bash
# Terminal 1: Start mock service
python3 scripts/mock_service.py 8080

# Terminal 2: Build DAR
daml build

# Terminal 3: Start Canton with bootstrap
canton -c canton.conf --bootstrap scripts/bootstrap.canton \
  -Ddar.path=.daml/dist/external-call-integration-test-1.0.0.dar

# Terminal 4: Run test (after bootstrap completes)
canton -c canton.conf --script scripts/run_test.canton \
  -Ddar.path=.daml/dist/external-call-integration-test-1.0.0.dar
```

## Expected Results

### Mock Service Logs

You should see exactly **one** request with `mode=submission`:

```
>>> REQUEST #1
    Mode: submission
    Function: echo
    Input: aabbccdd
    Request ID: ...
```

No requests with `mode=validation` should appear (observer uses stored results).

### Test Output

```
============================================================
TEST RESULTS SUMMARY
============================================================

  [PASS] Contract created with signatory and observer
  [PASS] External call executed during choice exercise
  [PASS] Result correctly echoed back
  [PASS] Observer (participant2) saw the transaction
  [PASS] Observer processed tx WITHOUT external service
  [PASS] Multiple external calls work correctly

============================================================
ALL TESTS PASSED!
============================================================
```

## Configuration Details

### Participant1 (Signatory)

```hocon
parameters.engine {
  extensions {
    test-oracle {
      host = "127.0.0.1"
      port = 8080
      # ... full extension config
    }
  }
}
```

### Participant2 (Observer)

```hocon
parameters.engine {
  # INTENTIONALLY NO EXTENSIONS CONFIGURED
  # Must use stored results from transactions
}
```

## Troubleshooting

### "Extension not configured" error

- Make sure the mock service is running on port 8080
- Check that participant1's config has the extension configured

### Participant2 fails to process transaction

- This would indicate a bug in stored result handling
- Check that external call results are being stored in ActionDescription
- Verify ModelConformanceChecker is extracting and passing stored results

### Mock service shows validation requests

- This means the stored result replay isn't working
- Check DAMLe.scala's handling of ResultNeedExternalCall
- Verify storedExternalCallResults is being passed to reinterpret

## Files Explained

### canton.conf

Canton configuration with:
- `participant1`: Has extension service configured (makes HTTP calls)
- `participant2`: No extension configured (must use stored results)
- Single sequencer and mediator for the synchronizer

### mock_service.py

Python HTTP server that:
- Echoes input as output for `echo` function
- Counts requests by mode (submission vs validation)
- Writes counts to `/tmp/external_call_test_counts.json`

### bootstrap.canton

Canton script that:
- Starts all nodes
- Bootstraps the synchronizer
- Connects both participants
- Allocates Alice (on p1) and Bob (on p2)
- Uploads the DAR

### run_test.canton

Canton script that:
- Creates contract with Alice as signatory, Bob as observer
- Exercises choice with external call
- Verifies both participants see the transaction
- Checks mock service call counts
