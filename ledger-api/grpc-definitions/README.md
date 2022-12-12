# Ledger API gRPC definitions

The definitions in this package depend on Protobuf's ["well known types"](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf), as well as:

- https://raw.githubusercontent.com/grpc/grpc/v1.18.0/src/proto/grpc/status/status.proto
- https://raw.githubusercontent.com/grpc/grpc/v1.18.0/src/proto/grpc/health/v1/health.proto

## Building external to Bazel
You will need to download the base Google protobuf definitions and place these inside the folder `google/protobuf`. The minimum inclusions that you will require to gather will be:
- https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/any.proto 
- https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/duration.proto
- https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/wrappers.proto
- https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto

Optional proto files that you may require:
- https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/descriptor.proto
- https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/empty.proto
- https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/field_mask.proto
- https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/timestamp.proto
