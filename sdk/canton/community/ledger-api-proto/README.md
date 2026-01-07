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

# Terminology

## The participant's offset

Describes a specific point in the stream of updates observed by this participant.
A participant offset is meaningful only in the context of its participant. Different
participants may associate different offsets to the same change synchronized over a synchronizer,
and conversely, the same literal participant offset may refer to different changes on
different participants.

This is also a unique index of the changes which happened on the virtual shared ledger.
The order of participant offsets is reflected in the order the updates that are
visible when subscribing to the `UpdateService`. This ordering is also a fully causal
ordering for any specific synchronizer: for two updates synchronized by the same synchronizer, the
one with a bigger participant offset happened after than the one with a smaller participant
offset. Please note this is not true for updates synchronized by different synchronizers.
Accordingly, the participant offset order may deviate from the order of the changes
on the virtual shared ledger.

The Ledger API endpoints that take offsets allow to specify portions
of the participant data that is relevant for the client to read.

Offsets returned by the Ledger API can be used as-is (e.g.
to keep track of processed transactions and provide a restart
point to use in case of failure).
