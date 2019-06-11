# DAML Tools

This compomnent provides anything that can reasonably be considered a DAML "Tool". Several of these are large enough to have a README in their own right, and those
are linked where available.

## DAML Command Line Interface

* `da-hs-daml-cli` is the Haskell library behind `damlc`
* `da-hs-damlc-app` provides `damlc`
* `damlc-jar:damlc.jar` packages up `damlc` for distribution inside a jar.

You can execute these directly with just

```
$ bazel run damlc -- <command line options>
```

due to the brief aliases specified in the `BUILD` file in the
root of our repository.


## DAML Studio

* [`daml-studio`](daml-studio/README.md) contains the DAML Studio plugin for Visual Studio Code

## Documentation

Contains internal documentation:

* `daml-contract-keys`: requirements and planning for contract keys, disclosure and contract quantities

* `daml-licenses`

* `time-model`
