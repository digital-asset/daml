# Working on `ghc-lib`

Copyright 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: (Apache-2.0 OR BSD-3-Clause)

If you need to build, test, deploy or develop [`ghc-lib`](https://github.com/digital-asset/ghc-lib) as used by DAML and utilizing the Digital Asset [GHC fork](https://github.com/digital-asset/ghc) these notes are for you.

## Table of contents
* [Prerequisites](#prerequisites)
* [How to build `ghc` from the DA GHC fork](#how-to-build-ghc-lib-from-the-da-ghc-fork)
* [How to build `ghc-lib` from the DA GHC fork](#how-to-build-ghc-lib-from-the-da-ghc-fork)
* [How to test `ghc-lib`](#how-to-test-ghc-lib)
* [How to deploy `ghc-lib`](#how-to-deploy-ghc-lib)
* [How to rebase `ghc-lib` on upstream master](#how-to-rebase-ghc-lib-on-upstream-master)
* [How to develop `ghc-lib`](#how-to-develop-ghc-lib)

## Prerequisites

- Download `stack` and other tools:
```bash
cd ~
mkdir -p ~/.local/bin
cat << EOF >> ~/.bashrc
export PATH=~/.local/bin:$PATH
EOF

brew install autoconf automake python3 gmp
curl -sSL https://get.haskellstack.org/ > install.sh
chmod +x install.sh && ./install.sh -f -d ~/.local/bin
ln -s /usr/bin/make ~/.local/bin/make
source ~/.bashrc
```

- Optional: So that you can generate a SHA (because this function is missing on MacOS), add this to your `~/.bashrc`:
```bash
function sha256sum() { shasum -a 256 "$@" ; } && export -f sha256sum
```

## How to build `ghc` from the DA GHC fork
To build DA's fork of `ghc` (which incorporates our extensions and DAML syntax):
```
mkdir -p ~/tmp && cd ~/tmp
git clone https://gitlab.haskell.org/ghc/ghc.git
cd ghc
git remote add upstream git@github.com:digital-asset/ghc.git
git fetch upstream
git checkout `git merge-base upstream/da-master master`
git merge --no-edit upstream/da-master
git submodule update --init --recursive
stack build --stack-yaml=hadrian/stack.yaml --only-dependencies
hadrian/build.stack.sh --configure --flavour=quickest -j
```
The compiler is built to `_build/stage1/bin/ghc`.

Note that the `git checkout` step will put you in detached HEAD state - that's expected.

## How to build `ghc-lib` from the DA GHC fork
(You don't need to follow the previous step in order to do this.)

These instructions detail how to generate `ghc-lib-parser` and `ghc-lib` packages intended for use by `damlc`.

1. Generate `ghc-lib-parser.cabal` by running:
```bash
mkdir -p ~/tmp && cd ~/tmp
git clone git@github.com:digital-asset/ghc-lib.git
cd ghc-lib && git clone https://gitlab.haskell.org/ghc/ghc.git
cd ghc
git remote add upstream git@github.com:digital-asset/ghc.git
git fetch upstream
git checkout `git merge-base upstream/da-master master`
git merge --no-edit upstream/da-master upstream/da-unit-ids
git submodule update --init --recursive
cd ..
stack setup > /dev/null 2>&1
stack build --no-terminal --interleaved-output
stack exec -- ghc-lib-gen ghc --ghc-lib-parser
```
Note that the `git checkout` step will put you in detached HEAD state - that's expected.

2. Edit `~/tmp/ghc-lib/ghc/ghc-lib-parser.cabal` to (a) change the version number (we use a datestamp, e.g. `0.20190219`) and (b) add clause `extra-libraries:ffi` to the `library` stanza. Then run:
```bash
cat << EOF >> stack.yaml
- ghc
EOF
stack sdist ghc --tar-dir=.
```
This creates `~tmp/ghc-lib/ghc-lib-parser-xxx.tar.gz` where `xxx` is the version number.

3. Generate `ghc-lib.cabal` by running:
```bash
(cd ghc && git clean -xf && git checkout .)
stack exec -- ghc-lib-gen ghc --ghc-lib
```

4. Edit `~/tmp/ghc-lib/ghc/ghc-lib.cabal` to (a) change the version number (we use a datestamp, e.g. `0.20190219`), (b) change the `ghc-lib-parser` version number in the `build-depends` stanza and (c) add clause `extra-libraries:ffi` to the `library` stanza. Then run:
```
stack sdist ghc --tar-dir=.
```
This creates `~tmp/ghc-lib/ghc-lib-xxx.tar.gz` where `xxx` is the version number.

5. You can (optionally) test that `ghc-lib-parser` and `ghc-lib` sdists build with these commands:
```bash
tar xvf ghc-lib-parser-xxx.tar.gz
tar xvf ghc-lib-xxx.tar.gz
mv ghc-lib-parser-xxx ghc-lib-parser
mv ghc-lib-xxx ghc-lib
sed '$d' stack.yaml > stack.yaml.tmp&&cp stack.yaml.tmp stack.yaml
cat << EOF >> stack.yaml
- ghc-lib-parser
- ghc-lib
EOF
stack build ghc-lib-parser --no-terminal --interleaved-output
stack build ghc-lib --no-terminal --interleaved-output
```
where, as in steps 3 and 4, `xxx` is the version number.

## How to test `ghc-lib`
Once you've [built `ghc-lib`](#how-to-build-ghc-lib-from-the-da-ghc-fork), you should test it locally:

1. Get the SHAs of the tar.gz files. If you followed the last step in the prerequsites, you can do this by running:
```
sha256sum ghc-lib-parser-xxx.tar.gz
sha256sum ghc-lib-xxx.tar.gz
```
where as before, `xxx` is the version number.

2. At the root of the `daml` repo, edit `WORKSPACE` (determines where Bazel gets `ghc-lib` from).
Update the lines for `ghc-lib-parser` and `ghc-lib` with the new `url`s, `stripPrefix`s and `sha256s`:
```bash
  ("ghc-lib-parser", {"url": "file:///path/to/the/ghc-lib-parser-xxx.tar.gz", "stripPrefix": "ghc-lib-parser-xxx", "sha256": "a422c86eaf6efe7cec8086b1b0f361355d4415825cf0513502755736a191ab44"})
, ("ghc-lib", {"url": "file:///path/to/the/ghc-lib-xxx.tar.gz", "stripPrefix": "ghc-lib-xxx", "sha256": "d422c86eaf6efe7cec8086b1b0f361355d4415825cf0513502755736a191ab66"})
```
3. Check that the DAML tests pass:
```bash
bazel run //daml-foundations/daml-ghc:daml-ghc-test -- --pattern=
```
If they pass, you can move on to [deploying](#how-to-deploy-ghc-lib).

## How to deploy `ghc-lib`
Now you've [built](#how-to-build-ghc-lib-from-the-da-ghc-fork) and [tested `ghc-lib`](#how-to-test-ghc-lib), you can deploy it:

1. Upload `ghc-lib-parser-xxx.tar.gz` and `ghc-lib-xxx.tar.gz` to [bintray](https://bintray.com/digitalassetsdk/ghc-lib) with commands like the following
```bash
API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx;export API_KEY

# Upload commands are formed as follows.
#   curl -T <FILE.EXT> -ushayne.fletcher@digitalassetsdk:<API_KEY> \
#   https://api.bintray.com/content/digitalassetsdk/ghc-lib/<YOUR_COOL_PACKAGE_NAME>/<VERSION_NAME>/<FILE_TARGET_PATH>
curl -T /path/to/ghc-lib-parser-xxx.tar.gz \
     -ushayne.fletcher@digitalassetsdk:$API_KEY \
     https://api.bintray.com/content/digitalassetsdk/ghc-lib/da-ghc-lib/xxx/ghc-lib-parser-xxx.tar.gz
curl -T /path/to/ghc-lib-xxx.tar.gz \
     -ushayne.fletcher@digitalassetsdk:$API_KEY \
     https://api.bintray.com/content/digitalassetsdk/ghc-lib/da-ghc-lib/xxx/ghc-lib-xxx.tar.gz
curl -X POST -ushayne.fletcher@digitalassetsdk:$API_KEY \
     https://api.bintray.com/content/digitalassetsdk/ghc-lib/da-ghc-lib/xxx/publish
```
(where `API_KEY` is replaced by your bintray `API_KEY` which you can retrieve by looking into your bintray profile).

2. In the `daml` repo, create a new feature branch:
```bash
cd daml
git checkout -b update-ghc-lib
```
3. Edit `WORKSPACE` _again_ with the `url`s pointing to bintray this time:
```bash
# Download commands are formed as follows.
#   curl -L "https://digitalassetsdk.bintray.com/ghc-lib/<FILE_PATH>" -o <FILE.EXT>
# Example download URL - https://digitalassetsdk.bintray.com/ghc-lib/ghc-lib-xxx.tar.gz.
          ("ghc-lib-parser", {"url": "https://digitalassetsdk.bintray.com/ghc-lib/ghc-lib-parser-0.20190401.1.tar.gz", "stripPrefix": "ghc-lib-parser-0.20190401.1", "sha256": "3036ed084ca57668faab25f8ae0420a992e21ad484c6f82acce73705dfed9e33"})
        , ("ghc-lib", {"url": "https://digitalassetsdk.bintray.com/ghc-lib/ghc-lib-0.20190401.1.tar.gz", "stripPrefix": "ghc-lib-0.20190401.1", "sha256": "82e94f26729c35fddc7a3d7d6b0c89f397109342b2c092c70173bb537af6f5c9"})
```
4. If you didn't do this before, make sure the DAML tests pass by running:
```bash
bazel run //daml-foundations/daml-ghc:daml-ghc-test -- --pattern=
```
5. When the tests pass, push your branch to origin and raise a PR.

## `ghc-lib` in CI

At this time we have a pipeline in Jenkins [here](https://ci2.da-int.net/job/daml/job/ghc-lib/). It is run on a cron and can be run on demand.

## How to rebase `ghc-lib` on upstream master

To keep `ghc-lib` consistent with changes to upstream GHC source code, it is neccessary to rebase our branches on the upstream `master` from time to time. The procedure for doing this is as follows:
```bash
mkdir -p ~/tmp && cd ~/tmp
git clone git@github.com:digital-asset/ghc.git
cd ghc
git remote add upstream https://gitlab.haskell.org/ghc/ghc.git
git fetch upstream master
# These checkout commands take into account that `da-master` is the "default" branch.
git checkout -t origin/master && git merge upstream/master
git checkout da-master && git rebase master
git checkout -t origin/da-unit-ids && git rebase master
```
Obviously, you will need to deal with any rebase conflicts that come up (hopefully not often). You can test `ghc-lib` after rebasing by following the [build procedure](#how-to-build-ghc-lib-from-the-da-ghc-fork) replacing the line
```bash
git remote add upstream git@github.com:digital-asset/ghc.git
```
with
```bash
git remote add upstream $HOME/tmp/ghc
```
and then the [test procedure](#how-to-test-ghc-lib).

When you are satisfied that the tests pass, you can push the changes to origin with these commands:
```bash
cd ~/tmp/ghc
git push origin master:master
git push -f origin da-master:da-master
git push -f origin da-unit-ids:da-unit-ids
```
After this, release the updated `ghc-lib` following the usual [deployment procedure](#how-to-deploy-ghc-lib).

## How to develop `ghc-lib`

The following procedure sets up a new feature branch with starting point `da-master`.
```bash
mkdir ~/tmp && cd ~/tmp
git clone https://gitlab.haskell.org/ghc/ghc.git ghc.git
cd ghc.git
git remote add upstream git@github.com:digital-asset/ghc.git
git fetch upstream da-master
git checkout -t upstream/da-master
git checkout -b feature-xxx da-master
git push upstream feature-xxx:feature-xxx
```
where `feature-xxx` is replaced by the desired name of your feature branch.

To prepare to produce a `ghc` from your feature branch, remember to first initialize submodules and build hadrian's dependencies (hadrian itself will be built on the first ghc build invocation).
```
git submodule update --init --recursive
stack build --stack-yaml=hadrian/stack.yaml --only-dependencies
```
To build `ghc` invoke hadrian via `hadrian/build.stack.sh`.
```bash
hadrian/build.stack.sh --configure --flavour=quickest -j
```
As usual, the compiler is built to `_build/stage1/bin/ghc`.

When you are ready to publish your feature branch, push to `upstream` and raise your PR with base `da-master`.
