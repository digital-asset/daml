# Ledger-API

This is the API code for the ledger, which contains:
* gRPC API definitions
* Generated Scala bindings and client
* gRPC-RS bridge
* gRPC-Pekko bridge
* Server API classes with validation
* Prototype Server
* Integration tests for all of the above

# Documentation

The [Ledger API Introduction](https://docs.daml.com/app-dev/grpc/index.html) contains introductory material as well as links to the protodocs reference documentation.

The reference documentation is auto-generated from the protoc comments in the *.proto files.
The comments must be [compatible with reStructuredText](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html). For example:

```
Long paragraphs can be wrapped
across multiple lines.

Place blank lines between paragraphs.

1. Lists require a blank line before and after the list.
2. Multiline list items
   must have consistent
   indentation before each line.
3. Numbered lists cannot be defined with (1), (2), etc.

Comments that are not RST-compatible will fail the automated build process.
```

# Dependencies

The definitions in this package depend on Protobuf's ["well known types"](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf), as well as:

- https://raw.githubusercontent.com/grpc/grpc/v1.18.0/src/proto/grpc/status/status.proto
- https://raw.githubusercontent.com/grpc/grpc/v1.18.0/src/proto/grpc/health/v1/health.proto

The [Metering Report Json Schema](docs/metering-report-schema.json) is also included.
