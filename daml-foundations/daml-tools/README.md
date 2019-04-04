# DAML Tools

This compomnent provides anything that can reasonably be considered a DAML "Tool". Several of these are large enough to have a README in their own right, and those
are linked where available.

## DAML Command Line Interface

* `da-hs-daml-cli` is the Haskell library behind `damli` and `damlc`
* `da-hs-damlc-app` provides `damlc`
* `da-hs-damli-app` provides `damli`
* `damlc-jar:damlc.jar` packages up `damlc` for distribution inside a jar.

You can execute these directly with just

```
$ bazel run damlc -- <command line options>
```

or

```
$ bazel run damli -- <command line options>
```

due to the brief aliases specified in the `BUILD` file in the
root of our repository.


## DAML Studio

* [`daml-studio`](daml-studio/README.md) contains the DAML Studio plugin for Visual Studio Code

## Tests

* [`language-server-tests`](language-server-tests/README.md) contains bit-rotted tests for the DAML language server.

## Documentation

Contains internal documentation:

* `daml-contract-keys`: requirements and planning for contract keys, disclosure and contract quantities

* `daml-lf-specification`

* `daml-licenses`

* `time-model`
