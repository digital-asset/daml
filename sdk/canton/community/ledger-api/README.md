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

# Dependencies

The definitions in this package depend on Protobuf's ["well known types"](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf), as well as:

- https://raw.githubusercontent.com/grpc/grpc/v1.18.0/src/proto/grpc/status/status.proto
- https://raw.githubusercontent.com/grpc/grpc/v1.18.0/src/proto/grpc/health/v1/health.proto

The [Metering Report Json Schema](docs/metering-report-schema.json) is also included.
