## Code layout

The following list is ordered topologicaly based on the dependency graph.

### daml-preprocessor

`daml-preprocessor` contains the Daml preprocessor which runs our version of the
`record-dot-preprocessor` and the preprocessor for generating
`Generic` instances. The preprocessor also performs a few additional
checks, e.g., that you do not import internal modules.

### daml-opts

`daml-opts` contains two libraries: `daml-opt-types` and `daml-opts`.

`daml-opt-types` contains the `Options` type which controls the
various flags affecting most `damlc` commands. Most of the options can
be controlled via command line flags.

`daml-opts` contains the conversion from `damlc`’s `Options` type to
`ghcide`’s `IdeOptions` type. This is in a separate package to avoid
making everything depend on `daml-preprocessor`.

### daml-lf-conversion

`daml-lf-conversion` handles the conversion from GHC’s Core to Daml-LF.

### daml-ide-core

`daml-ide-core` is a wrapper around `ghcide` that adds Daml-specific
rules such as rules for producing `Daml-LF`.


### daml-doc

`daml-doc` contains our variant of `haddock`.


### daml-desugar

`daml-desugar` provides a simple way to obtain the GHC representation of a
parsed Daml file.


### daml-ide

`daml-ide` contains the LSP layer of the IDE and wraps the
corresponding LSP layer in `ghcide` and adds custom handlers such as
those for scenario results.

### daml-compiler

`daml-compiler` contains the implementation of a few top-level `damlc`
commands, e.g., `upgrade`.

### lib

`lib` is all of `damlc` but packaged as a library since that can be
more convenient for tests.

### exe

This is a tiny wrapper around `lib` to produce the `damlc` executable.

## Developing

When working on the compiler:

```
da-ghcid //compiler/damlc/tests:integration-v1dev --reload=compiler/damlc/tests/daml-test-files --test=":main --pattern="
bazel run //compiler/damlc/tests:integration-v1dev -- --pattern=
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


## Updating expected diagnostics in the damlc integration tests

Most of the `.daml` files in `tests/daml-test-files` contain comments of the
form
```haskell
-- @WARN range=1:1-1:10; Something to warn about
```
These comments specify the diagnostics we expect when compiling the file.
Sometimes these expectations change for various reasons and updating the
comments can be quite tedious, particularly when there are many of them.
You can extract the comments reflecting the updated expectations from the
compiler output by copying the output into the clipboard and running
```sh
# On MacOS
pbpaste | sed -n -r -f compiler/damlc/tests/extract-diagnostics.sed

# On Linux
xclip -out -selection clipboard | sed -n -r -f compiler/damlc/tests/extract-diagnostics.sed
```
If a test case in the `damlc` intgration tests fails, it will print the
compiler output. Alternative, you can run `damlc` as described above to get
the output.


## Updating daml-doc's golden tests

Run
```
bazel run //compiler/damlc/tests:daml-doc -- --accept
```
to accept the current documentation as new golden files.

## Updating daml-desugar's golden tests

Likewise, run
```
bazel run //compiler/damlc/tests:daml-desugar -- --accept
```
to accept the current desugaring as new golden files.

## Documentation

Standard library docs are exposed under the bazel rules which you can build with:

```
bazel build //compiler/damlc:daml-base-docs
```

This creates a tarball containing RST (ReStructured Text) docs, and a hoogle database.

## Daml Packages and Database

A Daml project is compiled to a Daml package and can be distributed as a Daml archive (DAR). This is
essentially a zip archive containing the Daml source code of the library together with the compiled
.dalf file. The damlc package loading mechanism is based on GHC's package database
and uses the same .conf file format. GHC's package
database is documented at
https://downloads.haskell.org/~ghc/latest/docs/html/users_guide/packages.html.

### Loading packages

`damlc` loads packages from a package database given by the option `--package-db`. It creates a
map from package name to Daml-LF file from all the contained .dalf files in this directory and links
the created Daml-LF against these packages. It uses the .hi interface files created upon
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
