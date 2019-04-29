# Ledger API Test Tool

*Status: ALPHA*

This is a standalone command line tool for testing the correctness of DAML ledger
implementations.

## Development

### Running via Bazel

To build and run the tool, use:

    bazel run //ledger/ledger-api-test-tool:ledger-api-test-tool

It wraps
[SemanticTester.scala](../../daml-lf/testing-tools/src/main/scala/com/digitalasset/daml/lf/engine/testing/SemanticTester.scala)
into a standalone command line tool with embedded
`//ledger/ledger-api-integration-tests:SemanticTests.dar`.

### Running standalone

Run

    bazel build //ledger/ledger-api-test-tool:ledger-api-test-tool_deploy.jar

to a "fat" JAR at
`bazel-bin/ledger/ledger-api-test-tool/ledger-api-test-tool_deploy.jar` which
can be run with:

    java -jar bazel-bin/ledger/ledger-api-test-tool/ledger-api-test-tool_deploy.jar

### Publishing

The tool is automatically released as part of SDK releases into paths like (note
the version string in the url)
https://bintray.com/digitalassetsdk/DigitalAssetSDK/sdk-components/100.11.31#files/com%2Fdaml%2Fledger%2Ftesttool

## Usage

See [Ledger API Test Tool
docs](../../docs/source/tools/ledger-api-test-tool/index.rst) for usage docs.
Publicly released docs are available at
https://docs.daml.com/tools/ledger-api-test-tool/index.html

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

