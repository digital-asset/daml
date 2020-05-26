# Ledger API authorization

## General authorization in gRPC

An `Interceptor` reads HTTP headers, and stores relevant information (e.g., claims) in a `Context`.

GRPC services read the stored data from the `Context` in order to validate the requests.

## Authorization in the ledger API

The `AuthService` defines an interface for decoding HTTP headers into `Claims`.

The ledger API server takes an `AuthService` implementation as an argument.

The ledger API server uses a call interceptor and the given `AuthService` implementation to to store decoded `Claims` in the gRPC `Context`.

All ledger API services use the `Claims` to validate their requests.
