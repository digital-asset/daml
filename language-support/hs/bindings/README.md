# Usage

There is currently no separate documentation for the haskell ledger bindings,
over and above the existing ledger API doc.

The [.proto files](/ledger-api/grpc-definitions/com/digitalasset/ledger/api/v1)
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

These bindings can be build as a standalone Haskell package `daml-ledger` using `stack`. Currently, `stack` references the code directly in the `daml` repo. The only annoyance is that part of the Haskell code for `daml-ledger` is generated from `.proto` files, and this generation step must be performed using the `bazel` build. We plan to simplify this step by directly providing `daml-ledger` as a package on hackage.

As well as `stack`, the only other prerequisite is to have `grpc` installed.
We are currently using gRPC version `1.23.0`. To install `grpc`  requires building `grpc` from source (really!). See detailed instructions here: https://github.com/grpc/grpc/blob/master/BUILDING.md.

Note, the `grpc` build instructions warn against doing a global install:

> *"WARNING: After installing with make install there is no easy way to uninstall, which can cause issues if you later want to remove the grpc and/or protobuf installation or upgrade to a newer version."*

If you follow this advice, then you must adjust `extra-lib-dirs` in your `stack.yaml` config. This is explicated in the detailed instructions below.


## Download (somewhere permanent) and build grpc at version 1.23.0

    git clone -b v1.23.0 https://github.com/grpc/grpc
    cd grpc
    GRPC=$(pwd)                         # for later
    git submodule update --init
    make

## Clone daml repo; and export the daml-ledger package

    cd /tmp
    git clone https://github.com/digital-asset/daml.git
    cd daml
    direnv allow
    language-support/hs/bindings/export-package.sh /tmp

## Write a DAML Ledger App in Haskell (or copy one!)

    cd /tmp
    cp -rp /tmp/daml/language-support/hs/bindings/examples/nim nim
    cd nim

## Adjust `stack.yaml` to find `grpc`

    sed -i 's,/usr/local/grpc/lib,'$GRPC'/libs/opt,' stack.yaml

## Build

    stack build
