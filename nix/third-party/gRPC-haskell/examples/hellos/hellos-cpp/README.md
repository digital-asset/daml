# gRPC C++ hellos example

This directory contains C++ client/server code for the "hellos" streaming mode
example.

## Building

Just run make.

## Usage

For running the C++ client and server against each other,

$ make
$ ./hellos_server &
$ ./hellos_client

For running the C++ client against the Haskell server

$ stack build
$ make
$ stack exec hellos-server &
$ ./hellos_client
