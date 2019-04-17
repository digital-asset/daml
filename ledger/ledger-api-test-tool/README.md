# Ledger API Test Tool

*Status: ALPHA*

This is a standlone command line tool for testing the correctness of DAML ledger
implementations.

## Development

To build and run the tool, use:

    bazel run //ledger/ledger-api-test-tool:ledger-api-test-tool

It wraps
[SemanticTester.scala](../../daml-lf/testing-tools/src/main/scala/com/digitalasset/daml/lf/engine/testing/SemanticTester.scala)
into a standalone command line tool with embedded
`//ledger/ledger-api-integration-tests:SemanticTests.dar`.

## Usage

See [Ledger API Test Tool
docs](../../docs/source/tools/ledger-api-test-tool/index.rst) for usage docs. Public docs are available at https://docs.daml.com/tools/ledger-api-test-tool/index.html

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

