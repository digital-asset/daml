# `proto3-suite`

[![Build Status](https://travis-ci.org/awakesecurity/proto3-suite.svg?branch=master)](https://travis-ci.org/awakesecurity/proto3-suite)

This package defines tools for working with protocol buffers version 3 in Haskell.

This library provides a higher-level API to
[the `proto3-wire` library](https://github.com/awakenetworks/proto3-wire) that supports:

- Type classes for encoding and decoding messages, and instances for all
  wire formats identified in the specification
- A higher-level approach to encoding and decoding, based on `GHC.Generics`
- A way of creating `.proto` files from Haskell types.

See the `Proto3.Suite.Tutorial` module for more details.

## Running the language interop tests

We test inter-language interop using protoc's built-in Python code generation. In
order to successfully run these tests, you'll need to install the google protobuf
Python library. It's best to create a virtualenv and then use pip to install the
right version (virtualenv is a python utility which can be installed with pip).

```bash
$ virtualenv pyenv
$ source pyenv/bin/activate
$ pip install protobuf==3.0.0b3  # Need the latest version for the newest protoc
```

`brew install python` may also work.

Alternately, the `nix-shell` environment provides an incremental build
environment (but see below for testing). From the root of this repository:

```bash
$ nix-shell release.nix -A proto3-suite-no-tests.env
[nix-shell]$ cabal configure
[nix-shell]$ cabal build
```

Once your source code compiles and you want to test, do this instead:

```bash
$ nix-shell release.nix -A proto3-suite.env
[nix-shell]$ cabal configure --enable-tests
[nix-shell]$ cabal build
[nix-shell]$ cabal test
```

The above steps will work only if your Haskell source compiles, because
some of the tests require the current `compile-proto-file` executable.

## `compile-proto-file` and `canonicalize-proto-file` installation

Run the following commmand from the root of this repository to install
the `compile-proto-file` and `canonicalize-proto-file` executables:

```bash
$ nix-env --install --attr proto3-suite -f release.nix
```

To remove it from your nix user profile path, use:

```bash
$ nix-env --uninstall proto3-suite
```

## `compile-proto-file` usage

```bash
$ compile-proto-file --help
Compiles a .proto file to a Haskell module

Usage: compile-proto-file --out FILEPATH [--includeDir FILEPATH]...
                          --proto FILEPATH

Available options:
  -h,--help                Show this help text
  --out FILEPATH           Output directory path where generated Haskell modules
                           will be written (directory is created if it does not
                           exist; note that files in the output directory may be
                           overwritten!)
  --includeDir FILEPATH... Path to search for included .proto files (can be
                           repeated, and paths will be searched in order; the
                           current directory is used if this option is not
                           provided)
  --proto FILEPATH         Path to input .proto file
```

`compile-proto-file` bases the name (and hence, path) of the generated Haskell
module on the filename of the input `.proto` file, _relative_ to the include
path where it was found, snake-to-cameling as needed.

As an example, let's assume this is our current directory structure before
performing any code generation:

```
.
├── my_protos
│   └── my_package.proto
└── other_protos
    └── google
        └── protobuf
            ├── duration.proto
            └── timestamp.proto
```

where `my_package.proto` is:

```
syntax = "proto3";
package some_package_name;
import "google/protobuf/timestamp.proto";
import "google/protobuf/duration.proto";
message MyMessage {
  Timestamp timestamp = 1;
  Duration  duration  = 2;
}
```

Then, after the following commands:

```bash
$ compile-proto-file --out gen --includeDir my_protos --includeDir other_protos --proto google/protobuf/duration.proto
$ compile-proto-file --out gen --includeDir my_protos --includeDir other_protos --proto google/protobuf/timestamp.proto
$ compile-proto-file --out gen --includeDir my_protos --includeDir other_protos --proto my_package.proto
```

the directory tree will look like this:

```
.
├── gen
│   ├── Google
│   │   └── Protobuf
│   │       ├── Duration.hs
│   │       └── Timestamp.hs
│   └── MyPackage.hs
├── my_protos
│   └── my_package.proto
└── other_protos
    └── google
        └── protobuf
            ├── duration.proto
            └── timestamp.proto
```

Finally, note that delimiting `.` characters in the input `.proto` basename are
treated as `/` characters, so the input filenames
`google.protobuf.timestamp.proto` and `google/protobuf/timestamp.proto` would
produce the same generated Haskell module name and path.

This is essentially the same module naming scheme as the `protoc` Python plugin
uses when compiling `.proto` files.
