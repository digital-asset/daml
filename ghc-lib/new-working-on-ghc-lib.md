Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
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

3. Checkout the version of interest (usually `da-master-8.8.1`, or
`da-master-8.8.1-daml-2.x` if you're targetting (daml) `main-2.x`) which should
match `GHC_REV` from [`$DAML_REPO/bazel_tools/ghc-lib/version.bzl`](https://github.com/digital-asset/daml/blob/main/bazel_tools/ghc-lib/version.bzl))
and update the submodules:
```
git checkout da-master-8.8.1
git submodule update --init --recursive
git clean -xfdf
```

The last `git clean` is needed to remove the submodules from upstream master
that aren't used in our fork. `-xfdf` with two `f`s is needed because
submodules contain their own `.git` directories.

### Iterating on parser/desugaring in `ghc`

Working locally in a branch from `da-master-8.8.1`, there are two files which generally need changing to update syntax and desugaring:

- [`$GHC_REPO/compiler/parser/Parser.y`](https://github.com/digital-asset/ghc/blob/da-master-8.8.1/compiler/parser/Parser.y)

- [`$GHC_REPO/compiler/parser/RdrHsSyn.hs`](https://github.com/digital-asset/ghc/blob/da-master-8.8.1/compiler/parser/RdrHsSyn.hs)

The quickest way to build and test is:

1. `cd $GHC_REPO`

2. `direnv allow`, to enter the dev environment defined in `.envrc`. For help installing direnv, see [direnv](https://direnv.net). This should only be necessary the first time.

3. `hadrian/build.sh --configure --flavour=quickest -j`. This will give immediate feedback on build failures, but it takes about 2-3 minutes when successful.

4. `./_build/stage1/bin/ghc <EXAMPLE_FILE> -ddump-parsed | tee desugar.out`. `<EXAMPLE_FILE>` must be a file with extension `.hs`, which must begin with the pragma `{-# LANGUAGE DamlSyntax #-}`. A typical starting point is [`$GHC_REPO/Example.hs`](https://github.com/digital-asset/ghc/blob/da-master-8.8.1/Example.hs).

### Interactive development workflow

While working on GHC, you can integrate your changes directly into the `daml` project as follows, making sure to replace `$DAML_REPO` (resp. `$GHC_REPO`) with the path to the Daml (resp. GHC) repository on your machine:

1. Add `BUILD` file:
   ```
   ln -S $DAML_REPO/bazel_tools/ghc-lib/BUILD.ghc BUILD
   ```

2. Make changes... 🛠️

3. Build referencing your local checkout of GHC:
   ```
   cd $DAML_REPO
   bazel build --override_repository=da-ghc="$( cd $GHC_REPO ; pwd )" //...
   ```

### Pull request workflow

After you are satisfied with your changes,

1. Open a PR on the (DA) GHC repository.

2. Open a PR on the Daml repository, making sure that the SHA value in `GHC_REV` in [`$DAML_REPO/bazel_tools/ghc-lib/version.bzl`](https://github.com/digital-asset/daml/blob/main/bazel_tools/ghc-lib/version.bzl) corresponds to the last commit in the GHC PR.

3. To help your reviewers, please update the description of both PRs with a link to each other.

4. Once CI has passed on the Daml PR, you may merge the GHC PR.

5. Update the Daml PR so `GHC_REV` now points to the GHC PR **merge commit**.

6. Once CI has passed a second time, you may merge the Daml PR.
