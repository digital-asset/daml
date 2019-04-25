# `ex-haskell-grpc-arithmetic`

Basic example of using haskell-grpc. Nothing ledger specific here.

This example is derived from the tutorial example here: https://github.com/awakesecurity/gRPC-haskell/tree/master/examples/tutorial, setup to be build using bazel.
## Build and Run

	$ bazel build ...
	$ bazel run arith-server &
	$ bazel run arith-client
