# Usage

Using the ledger bindings outside of this repository can be a bit
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
