# DAML-GHC Project

The DAML 1.2 compiler, converting DAML as a dialect embedded into Haskell to DAML-LF.

## Compiling code

The code includes a simple CLI that compiles a given (main) DAML-1.2 file to LF.

```
-- Reserved for future use?
```

See the `--help` text for details.

## Developing

Before you start, build the IDE test suite. We fall back to this to
find runfiles such as the scenario service and the package database
when we’re running inside GHCi.

```
bazel build //daml-foundations/daml-ghc:daml-ghc-shake-test-ci
```

When working on the compiler:

```
da-ghcid //daml-foundations/daml-ghc:daml-ghc-test-newest --reload=daml-foundations/daml-ghc/tests --test=":main --pattern="
bazel run //daml-foundations/daml-ghc:daml-ghc-test-newest -- --pattern=
bazel run damlc -- compile $PWD/MyDaml12File.daml
```

When working on the IDE via the test suite:

```
bazel run //daml-foundations/daml-ghc:daml-ghc-shake-test-ci -- --pattern=
da-ghcid daml-foundations/daml-ghc/src/DA/Test/ShakeIdeClient.hs --test=":main --pattern="
```

Testing in situ:

```
da-sdk-head
da-sdk-head --damlc
```

## Documentation

Standard library docs are exposed under the bazel rules which you can build with:

```
bazel build //daml-foundations/daml-ghc:daml-base-rst-docs
bazel build //daml-foundations/daml-ghc:daml-base-hoogle-docs
```

Until we have fully automated doc generation, you can copy them out manually into the repo with the command:

```
cd ~/projects/daml
cp bazel-genfiles/daml-foundations/daml-ghc/daml-base-hoogle.txt docs/daml/stdlib/static/base.txt
cp bazel-genfiles/daml-foundations/daml-ghc/daml-base.rst docs/daml/stdlib/base.rst
cp bazel-genfiles/daml-foundations/daml-ghc/daml-base.rst daml-foundations/daml-stdlib/doc/source/base.rst
```

You can preview the results with:

```
cd ~/projects/daml/daml-foundations/daml-stdlib/doc
da-doc-preview
```

To see your changes, rebuild with bazel, rerun the last `cp` command, and visit the result at http://localhost:8081/base.html

## Deploying `ghc-lib`

Consult `nix/overrides/ghc-lib/README.md` for details on the `ghc-lib` nix package and how to test it.

## DAML Packages and Database

A DAML project is compiled to a DAML package and can be distributed as a DAML archive (DAR). This is
essentially a zip archive containing the DAML source code of the library together with the compiled
.dalf file. A full specification of the DAML archive format can be found in
`daml-foundations/daml-tools/docs/daml-project/source/reference.rst`. The daml-ghc package loading
mechanism is based on GHC's package database and uses the same .conf file format. GHC's package
database is documented at
https://downloads.haskell.org/~ghc/latest/docs/html/users_guide/packages.html.

### Installing a package
Currently there is no tooling for installing packages and the process is manual. Let's assume you
want to install a DAML archive `example` with the following directory structure:

```
example
├── DA
│   └── NewLibrary.daml
├── example.dalf
└── LibraryModules.daml
```

To install the DAML package `example.dar` into a package database directory, unzip the DAR file and
compile the LibraryModules.daml file with

```
unzip example.dar
cd example
bazel run damlc -- compile --package=example LibraryModules.daml -o example_compiled.dalf
```

Note that you need to specify the package name with the `--package` flag. The contained
`example.dalf` and the just compiled `example_compiled.dalf` DAML-LF files need to be the same file,
otherwise your archive is either corrupted or has been compiled with a different compiler. Copy all
the created files, in particular the created .hi interface files, to your package database
directory:

```
cd ..
cp -a example <package-db>/
```

Within the package database directory create a `example.conf` file with the following content:

```
name: example
version: 1.0.0
id: example
key: example
copyright: 2015-2019 Digital Asset Holdings
maintainer: Digital Asset
exposed: True
exposed-modules: <your exposed modules of the library>
import-dirs: example/
library-dirs: example/
data-dir: example/
depends: <dependencies on other packages>
```

Update the package cache with

```
ghc-pkg recache --package-db .
```

This creates a `package.cache` and a `package.cache.lock` file.

### Loading packages

`daml-ghc` loads packages from a package database given by the option `--package-db`. It creates a
map from package name to DAML-LF file from all the contained .dalf files in this directory and links
the created DAML-LF against these packages. It uses the .hi interface files created upon
installation of the packages for type checking.

### Base packages

Currently a package database is provided together with the `damlc` Bazel rule and `bazel run damlc`
loads this database by default. This package database is also shipped together with
`da-hs-damli-app.tar.gz` and `da-hs-damlc-app.tar.gz` for the SDK and the platform and is contained
in the directory `resources/package-db/gen/`.

### Building the package database
The package database that comes with daml-ghc and the above archives can be build with

```
bazel build //daml-foundations/daml-ghc/package-database:package-db
```

and the daml-prim.dalf for the daml-prim packge with

```
bazel build //daml-foundations/daml-ghc/package-database:daml-prim.dalf
```

## The `daml-blessed` tag

We should update the `daml-blessed` tag semi regularly. If you are asked to do that:

* Check when `daml-blesed` was [last updated](https://github.com/DACH-NY/da/releases/tag/daml-blessed), usually don't continue if it was recently
* Create a tag: `git tag -f -a daml-blessed -m "Creation of daml-blessed following the test plan"`
* Push it: `git push --force origin daml-blessed`
