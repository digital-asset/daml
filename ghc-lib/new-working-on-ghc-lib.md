Copyright 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: (Apache-2.0 OR BSD-3-Clause)

# Working on `ghc-lib`

If you need to build, test, deploy or develop [`ghc-lib`](https://github.com/digital-asset/ghc-lib) as used by DAML and utilizing the Digital Asset [GHC fork](https://github.com/digital-asset/ghc) these notes are for you.

Here are instructions for when working on DAML surface syntax, as implemented in the the digital-assert fork of `ghc`. (linked in via `ghc-lib`).


### Cloning the digital-assert fork of `ghc`

1. Make initial clone from the main ghc gitlab repo:
```
git clone --recurse-submodules https://gitlab.haskell.org/ghc/ghc.git
cd ghc
```

2. Add the DA fork as a remote
```
git remote add da-fork git@github.com:digital-asset/ghc.git
git fetch da-fork
```

3. Checkout the version of interest and update the submodules:
```
git checkout da-master-8.8.1
git submodule update --init --recursive
```

4. Made initial build (takes about 15 mins)
```
hadrian/build.stack.sh --configure --flavour=quickest -j
```


### Iterating on parser/desugaring in `ghc`

Working locally in a branch from `da-master-8.8.1`, there are two files which generally need changing to update syntax and desugaring:

- [`compiler/parser/Parser.y`](https://github.com/digital-asset/ghc/blob/da-master-8.8.1/compiler/parser/Parser.y)

- [`compiler/parser/RdrHsSyn.hs`](https://github.com/digital-asset/ghc/blob/da-master-8.8.1/compiler/parser/RdrHsSyn.hs)


The quickest way to build and test is:

1. `hadrian/build.stack.sh --configure --flavour=quickest -j`

2. `./_build/stage1/bin/ghc ./Example.hs -ddump-parsed | tee desugar.out`

Step 1 gives immediate feedback on build failures, but takes about 2-3 minutes when successful. For Step 2 you need a DAML example file. The input file must end in `.hs` suffix. It must begin with the pragma: `{-# LANGUAGE DamlSyntax #-}`


### Building `daml` following a change to `ghc`

Once you have the GHC patch you want to incorporate into the DAML repo, here's the steps you'll need to take:

1. Open a PR in the daml repo with the commit hash for the GHC patch in `ci/da-ghc-lib/compile.yml`. See [here](https://github.com/digital-asset/daml/pull/7489/commits/fedc456260f598f9924ce62d9765c3c09b8ad861)

2. Wait for CI to build `ghc-lib`/`ghc-lib-parser`, and get the new SHA from the end of the azure CI logs. The CI/azure log you are looking for is in the `Bash` subtab of the `da_ghc_lib` job. The lines of interest are at the very end of the log. See [here](https://dev.azure.com/digitalasset/adadc18a-d7df-446a-aacb-86042c1619c6/_apis/build/builds/60342/logs/52)

3. Update `stack-snapshot.yaml` with the new SHAs. See [here](https://github.com/digital-asset/daml/pull/7489/commits/f0198dc694238437357706c81b0c3d1979483d7a)

3. Run the pin command on linux or mac `bazel run @stackage-unpinned//:pin` and commit those changes as well

4. Before merging the PR, the pin command will also have to be run on windows, and those changes committed as well. You will need access to a windows machine for that: `ad-hoc.sh windows create`
