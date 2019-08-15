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

Using these bindings **outside of this repository** can be a bit
tricky since there are quite a few packages required that are not
published to Hackage at the moment.

To make things a bit easier there is a `build_packages.sh` script in
this directory that you can run as follows: `./build_packages.sh
TARGET_DIR`.  This will build the tarballs for all cabal packages and
put them in `TARGET_DIR`. If `TARGET_DIR` does not already contain a
`cabal.project` file, it will also create a `cabal.project` file that
references the created tarballs so all dependencies should be resolved
properly. You will also need to install gRPC 1.22, see
https://github.com/grpc/grpc/blob/master/BUILDING.md for installation
instructions. If you install it in a non-standard location, you need
to adjust extra-lib-dirs and extra-include-dirs for
grpc-haskell-core. The default `cabal.project` file contains an
example of how to do this.

The main package for the ledger bindings is called `daml-ledger`.
