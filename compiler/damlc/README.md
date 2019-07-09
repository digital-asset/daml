## Developing

When working on the compiler:

```
da-ghcid //compiler/damlc/tests:integration-dev --reload=compiler/damlc/tests/daml-test-files --test=":main --pattern="
bazel run //compiler/damlc/tests:integration-dev -- --pattern=
bazel run damlc -- compile $PWD/MyDaml12File.daml
```

When working on the IDE via the test suite:

```
bazel run //compiler/damlc/tests:shake -- --pattern=
da-ghcid //compiler/damlc/tests:shake --test=":main --pattern="
```

The above commands do not execute scenarios. To do that, use a command like
```
bazel run damlc test $PWD/compiler/damlc/tests/bond-trading/Test.daml
```

At the moment, commands relying on ghc-pkg, e.g., `damlc build` do not
work via `bazel run`. For testing, install the SDK with
`daml-sdk-head` and then use `daml-head damlc`.

## Documentation

Standard library docs are exposed under the bazel rules which you can build with:

```
bazel build //compiler/damlc:daml-base-rst-docs
bazel build //compiler/damlc:daml-base-hoogle-docs
```

## DAML Packages and Database

A DAML project is compiled to a DAML package and can be distributed as a DAML archive (DAR). This is
essentially a zip archive containing the DAML source code of the library together with the compiled
.dalf file. The damlc package loading mechanism is based on GHC's package database
and uses the same .conf file format. GHC's package
database is documented at
https://downloads.haskell.org/~ghc/latest/docs/html/users_guide/packages.html.

### Loading packages

`damlc` loads packages from a package database given by the option `--package-db`. It creates a
map from package name to DAML-LF file from all the contained .dalf files in this directory and links
the created DAML-LF against these packages. It uses the .hi interface files created upon
installation of the packages for type checking.

### Base packages

Currently a package database is provided together with the `damlc` Bazel rule and `bazel run damlc`
loads this database by default. This package database is also bundled in the `damlc-dist.tar.gz`
tarball included in the SDK.

### Building the package database
The package database that comes with damlc and the above archives can be build with

```
bazel build //compiler/damlc/pkg-db:pkg-db
```
