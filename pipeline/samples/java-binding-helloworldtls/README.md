# HelloWorldTls

## Scope

The purpose of this project is to provide a simple but representative project of Ledger API team, in particular to ensure Windows compatibility.

As the only native dependency used the by team is OpenSSL, this project only serves as a simple smoke test: if the project correctly builds and tests passes on Windows (as well as on Linux and macOS), we are reasonably sure about the absence of multi-platform compatibility issues.

This example is heavily based (and draws much code from) the official [gRPC TLS example](https://github.com/grpc/grpc-java/tree/v1.15.0/examples/src/main/java/io/grpc/examples/helloworldtls).

## Build

The project is built with Maven. To run the entire build process (including tests) simply run:

    mvn test

