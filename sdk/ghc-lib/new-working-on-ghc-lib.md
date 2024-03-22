Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: (Apache-2.0 OR BSD-3-Clause)

# Working on `ghc-lib`

If you need to build, test, deploy or develop [`ghc-lib`](https://github.com/digital-asset/ghc-lib) as used by Daml and utilizing the Digital Asset [GHC fork](https://github.com/digital-asset/ghc) these notes are for you.

Here are instructions for when working on Daml surface syntax, as implemented in the the digital-assert fork of `ghc`. (linked in via `ghc-lib`, see [ghc-lib](/bazel_tools/ghc-lib/).

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

### Iterating on parser/desugaring in `ghc`

Working locally in a branch from `da-master-8.8.1`, there are two files which generally need changing to update syntax and desugaring:

- [`compiler/parser/Parser.y`](https://github.com/digital-asset/ghc/blob/da-master-8.8.1/compiler/parser/Parser.y)

- [`compiler/parser/RdrHsSyn.hs`](https://github.com/digital-asset/ghc/blob/da-master-8.8.1/compiler/parser/RdrHsSyn.hs)


The quickest way to build and test is:

1. `hadrian/build.sh --configure --flavour=quickest -j`

2. `./_build/stage1/bin/ghc ./Example.hs -ddump-parsed | tee desugar.out`

Step 1 gives immediate feedback on build failures, but takes about 2-3 minutes when successful. For Step 2 you need a Daml example file. The input file must end in `.hs` suffix. It must begin with the pragma: `{-# LANGUAGE DamlSyntax #-}`


### Interactive development workflow

While working on GHC, you can integrate your changes directly into the `daml` project as follows.

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

3. Checkout the version of interest (usually `GHC_REV` from [`/bazel_tools/ghc-lib/version.bzl`]) and update the submodules:
   ```
   git checkout da-master-8.8.1
   git submodule update --init --recursive
   ```

4. Add `WORKSPACE` and `BUILD` file:
   ```
   cp ../daml/bazel_tools/ghc-lib/BUILD.ghc BUILD
   touch WORKSPACE
   ```

5. Make changes...  

6. Build referencing your local checkout of GHC:
   ```
   cd ../daml
   bazel build --override_repository=da-ghc="$( cd ../ghc ; pwd )" //...
   ```

After you are satisfied with your changes, just open a PR on the GHC repository and after it is merged update the SHA value in `GHC_REV` in [`/bazel_tools/ghc-lib/version.bzl`] and create a PR for the `daml` project.
