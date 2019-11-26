# daml2ts

This is a very early version of a code generator for a TypeScript interface
to a DAML package.  Its shortcomings are documented in this [Github
issue](https://github.com/digital-asset/daml/issues/3518).  It is not yet
shipped with the SDK. To run it, you must execute
```console
$ bazel run //:daml2ts
Usage: daml2ts DAR-FILE -o DIR
  Generate TypeScript bindings from a DAR
```
somehere in this resository. The `DAR-FILE` is the DAR for which you want to
generate the TypeScript interface.  `daml2ts` will generate interfaces for
all DALFs in that DAR. The output will be written into the directory `DIR`
you specify via the `-o` option.

Currently, the generated interfaces are tailored towards the
[ledger.ts](https://github.com/digital-asset/davl/blob/master/ui/src/ledger/ledger.ts)
module of DAVL.
