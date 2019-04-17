# Ledger API Test Tool

*Status: ALPHA*

The Ledger API Test Tool is a command line tool for testing the correctness of
ledger implementations based on DAML and the [Ledger
API](https://docs.daml.com/app-dev/ledger-api-introduction/index.html). It’s
useful for ledger implementation developers, who are using DAML Ledger
Implementation Kit to develop a DAML Ledger on top of their distributed-ledger
or database of choice.

Use this tool to verify if the Ledger API endpoint conforms to the [DA Ledger
Model](https://docs.daml.com/concepts/ledger-model/index.html).

## Development

To build and run the tool, use:

    bazel run //ledger/ledger-api-test-tool:ledger-api-test-tool

It wraps
[SemanticTester.scala](../../daml-lf/testing-tools/src/main/scala/com/digitalasset/daml/lf/engine/testing/SemanticTester.scala)
into a standalone command line tool with embedded
`//ledger/ledger-api-integration-tests:SemanticTests.dar`.

## Usage

*To be moved to SDK DAML documentation.*

### Downloading the tool

TODO

### Extracting `.dar` file required to run the tests

Before you can run the Ledger API test tool on your ledger, you need to load a
specific set of DAML templates onto your ledger.

1. Obtain the corresponding `.dar` file with:

        ledger-api-test-tool -x

2. Load the file `SemanticTests.dar` created in the current directory into your
   Ledger.

### Running the tool against a custom Ledger API endpoint

Run this command to test your Ledger API endpoint exposed at host `<host>` and
at a port `<port>`:

    ledger-api-test-tool -h <host> -p <port>

For example

    ledger-api-test-tool -h localhost -p 6865

### Exploring options the tool provides

Run the tool with `--help` flag to obtain the list of options the tool provides:

    $ ledger-api-test-tool --help
    The Ledger API Test Tool is a command line tool for testing the correctness of
    ledger implementations based on DAML and Ledger API.
    Usage: ledger-api-test-tool [options]

      --help                   prints this usage text
      -p, --target-port <value>
                               Ledger API server port. Defaults to 6865.
      -h, --host <value>       Ledger API server host. Defaults to localhost.
      --must-fail              One or more of the scenario tests must fail. Defaults to false.
      -r, --reset              Perform a ledger reset before running the tests. Defaults to false.
      -x, --extract            Extract the testing archive files and exit.

### Try out the Ledger API Test Tool against DAML Sandbox

To run the tool against [DAML
Sandbox](https://docs.daml.com/tools/sandbox.html), run:

    ledger-api-test-tool -x
    da sandbox -- SemanticTests.dar
    ledger-api-test-tool

This should always succeed! This is useful if you do not have yet a custom
Ledger API endpoint.

The DAML Sandbox starts at by default at `localhost:6865`
and the Ledger API Test Tool uses this as the default endpoint to test, hence
hosts and ports command line arguments can be omitted.


### Testing your tool from continuous integration pipelines

To test your ledger in your CI pipelines, execute it as part of your pipeline:

    $ ledger-api-test-tool
    Running 10 scenarios against localhost:6865...
    Testing scenario: Test:timeTravel
    Testing scenario: Test:authorization_rule1_failure
    Testing scenario: Test:authorization_success2_delegation
    Testing scenario: Test:authorization_rule2_failure
    Testing scenario: Test:startsAtEpoch
    Testing scenario: Test:authorization_success1
    Testing scenario: Test:privacy_projections1
    Testing scenario: Test:consistency_doublespend1
    Testing scenario: Test:consistency_doublespend3
    Testing scenario: Test:consistency_doublespend2
    All scenarios completed.
    $ echo $?
    0

The tool is tailored to be used in CI pipelines: as customary, when the tests
succeed, it will produce minimal output and return the success exit code.

### Connecting over TLS to a DAML Ledger, e.g. the Digital Asset Ledger

**NOT IMPLEMENTED**, see https://github.com/digital-asset/daml/issues/556 for
some extra context.

You might want to use this tool to verify that a deployment of Digital Asset
Ledger have been deployed correctly with regards to Ledger API conformance.

By default, Ledger API Test Tool is configured to use an unencrypted connection
to the ledger.

To run Ledger API Test Tool against a secured Digital Asset Ledger, configure
TLS certificates using the `--pem`, `--crt`, and `--cacrt` command line
parameters.

Details of these parameters are explained in the command line help:

    ledger-api-test-tool --help

### Using the tool with a known-to-be-faulty Ledger API implementation

To force the tool to always return success exit code, use `--must-fail` flag:

    ledger-api-test-tool --must-fail -h localhost -p 6865

This is useful during development of a DAML Ledger implementation, when tool
needs to be used against a known-to-be-faulty implementation (e.g. in CI).
