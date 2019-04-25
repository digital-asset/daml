# `ex-haskell-grpc-arithmetic`

Basic example of using haskell-grpc. Nothing ledger specific here.

This example is simply the tutorial example from: https://github.com/awakesecurity/gRPC-haskell/tree/master/examples/tutorial, setup to be build using bazel in the daml repo.

## Build and Run

	$ bazel build ...
	$ bazel run arith-server &
	$ bazel run arith-client
