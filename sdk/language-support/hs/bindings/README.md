# Usage

There is currently no separate documentation for the haskell ledger bindings,
over and above the existing ledger API doc.

The [.proto files](/canton/com/daml/ledger/api/v1)
are the best primary source of truth for the API. The [Haskell
bindings](/language-support/hs/bindings/src/DA/Ledger/Services) match closely
the names of the services and RPCs, but of course you get much better
[types](/language-support/hs/bindings/src/DA/Ledger/Types.hs).

The entry point is at [DA.Ledger](/language-support/hs/bindings/src/DA/Ledger.hs).

To use the bindings **in this repo**, you need the following `BUILD` dep:
```
"//language-support/hs/bindings:hs-ledger",
```
And then you can import the `DA.Ledger` module in your Haskell code.

You can find some usage examples
[here](/language-support/hs/bindings/test/DA/Ledger/Tests.hs) and
[here](/language-support/hs/bindings/examples/chat/src/DA/Ledger/App/Chat/ChatLedger.hs).


# Using these bindings **outside of this repository**

*Note: These instructions document the current process. We do plan make it simpler!*

These bindings can be exported as a standalone Haskell package `daml-ledger` using `stack`. Currently, `stack` references the code directly in the `daml` repo. The only annoyance is that part of the Haskell code for `daml-ledger` is generated from `.proto` files, and this generation step must be performed using the `bazel` build. We plan to simplify this step by directly providing `daml-ledger` as a package on hackage. Instructions for working with the daml repo can be found here: https://github.com/digital-asset/daml

As well as `stack`, the only other prerequisite is to have `grpc` installed. (`grpc` is not required to generate the `daml-ledger` package, but it is required to use it).

We are currently using gRPC version `1.23.0`. To install `grpc`  requires building `grpc` from source (really!). See detailed instructions here: https://github.com/grpc/grpc/blob/master/BUILDING.md.

In the instructions below we do `make install` for grpc despite the warning from the `grpc` build instructions:

> *"WARNING: After installing with make install there is no easy way to uninstall, which can cause issues if you later want to remove the grpc and/or protobuf installation or upgrade to a newer version."*

If you decide against the `make install`, or choose a different install location, you will need to adjust the settings of `extra-lib-dirs` and `extra-include-dirs` in your `stack.yaml` config.

Also, in the instructions below we export the `daml-ledger` package to `/tmp` which matches the location declared in the `stack.yaml` of the example application `nim`. If you export somewhere else, you will need to adapt your `stack.yaml`

## Download and build grpc at version 1.23.0

    git clone -b v1.23.0 https://github.com/grpc/grpc
    cd grpc
    git submodule update --init
    make
    make prefix=/usr/local/grpc install

## Install Nix

Follow the instructions from the [Nix manual][].

[Nix manual]: https://nixos.org/manual/nix/

## Clone daml repo, and export the daml-ledger package

    cd /tmp
    git clone https://github.com/digital-asset/daml.git
    cd daml
    language-support/hs/bindings/export-package.sh /tmp

## Write a Daml Ledger App in Haskell (or copy one!), and build it

    cd /tmp
    cp -rp /tmp/daml/language-support/hs/bindings/examples/nim nim
    cd nim
    stack build
