# Ledger API Test Tool

*Status: ALPHA*

The Navigator Console is a command line tool for testing correctness of ledger
implementations based on DAML and [DAML Ledger
API](https://docs.daml.com/app-dev/ledger-api-introduction/index.html). It’s
useful for ledger implementation developers, who are using DAML Ledger
Implementation Kit to develop a DAML Ledger on top of their distributed-ledger
or database of choice.

The tool will run a set of automated checks to verify if the target Ledger API
endpoint conforms to [DAML Ledger
Model](https://docs.daml.com/concepts/ledger-model/index.html), specifically:

* Consistency - for a transaction, internal consistency rules 1 and 2
* Authorization - for a commit, authorization rules 1 and 2
* Privacy - various test projections
* Divulgence
* Support for an exhaustive list of types and language constructs

## Usage

### Downloading the tool

TODO

### Extracting `.dar` file required to run the tests

Ledger API Test Tool depends on a specific set of DAML templates, which has to
be loaded into a DAML Ledger under test. You can obtain the `.dar` file with:

    ledger-api-test-tool -x

This will create file `SemanticTests.dar` in your local directory, which you
should load into your DAML Ledger.

### Try out the Ledger API Test Tool against DAML Sandbox

If you do not have yet a custom Ledger API endpoint, you can try running the
tool against [DAML Sandbox](https://docs.daml.com/tools/sandbox.html) with.

    ledger-api-test-tool -x
    da sandbox -- SemanticTests.dar
    ledger-api-test-tool

This should always succeed! 

### Running the tool against a Ledger API endpoint

Given a Ledger API server running at `<host>` and at a port `<port>`, one can
use the tool against it:

    ledger-api-test-tool -h <host> -p <port>

For example

    ledger-api-test-tool -h localhost -p 6865

### Using the tool in continuous integration pipelines

The tool is tailored to be used in CI pipelines. On success, it will produce
minimal output and return the success exit code:

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

### Running the tool with secured DAML Ledger, e.g. Digital Asset Ledger

**NOT IMPLEMENTED**

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

During development of a DAML Ledger implementation, it is possible that the tool
needs to be used against a known-to-be-faulty implementation (e.g. in CI). In
such cases use of `--must-fail` flag can be used:

    ledger-api-test-tool --must-fail -h localhost -p 6865
