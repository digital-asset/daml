Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: (Apache-2.0 OR BSD-3-Clause)

# Working on `ghc-lib`

If you need to build, test, deploy or develop [`ghc-lib`](https://github.com/digital-asset/ghc-lib) as used by Daml and utilizing the Digital Asset [GHC fork](https://github.com/digital-asset/ghc) these notes are for you.

Here are instructions for when working on Daml surface syntax, as implemented in the the digital-assert fork of `ghc`. (linked in via `ghc-lib`).


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

3. Set git URL rewrite rules

The `ghc` repo uses relative paths for its submodules, so things will break when using the `da-fork` remote. To avoid this, we tell git to use the address of the real submodule repos instead of the non-existent github repos it would get by following the relative paths:

```
git config --global url."ssh://git@gitlab.haskell.org/ghc/arcanist-external-json-linter.git".insteadOf git@github.com:digital-asset/arcanist-external-json-linter.git
git config --global url."ssh://git@gitlab.haskell.org/ghc/packages/".insteadOf git@github.com:digital-asset/packages/
```

4. Checkout the version of interest and update the submodules:
```
git checkout da-master-8.8.1
git submodule update --init --recursive
```

5. Make initial build (takes about 15 mins)
```
hadrian/build.stack.sh --configure --flavour=quickest -j
```

Note: on macOS, you might get errors related to the Integer library GMP. Until those are fixed, adding `--integer-simple` to the flags above should allow you to build GHC.

```
hadrian/build.stack.sh --configure --flavour=quickest -j --integer-simple
```

### Iterating on parser/desugaring in `ghc`

Working locally in a branch from `da-master-8.8.1`, there are two files which generally need changing to update syntax and desugaring:

- [`compiler/parser/Parser.y`](https://github.com/digital-asset/ghc/blob/da-master-8.8.1/compiler/parser/Parser.y)

- [`compiler/parser/RdrHsSyn.hs`](https://github.com/digital-asset/ghc/blob/da-master-8.8.1/compiler/parser/RdrHsSyn.hs)


The quickest way to build and test is:

1. `hadrian/build.stack.sh --configure --flavour=quickest -j`

2. `./_build/stage1/bin/ghc ./Example.hs -ddump-parsed | tee desugar.out`

Step 1 gives immediate feedback on build failures, but takes about 2-3 minutes when successful. For Step 2 you need a Daml example file. The input file must end in `.hs` suffix. It must begin with the pragma: `{-# LANGUAGE DamlSyntax #-}`


### Building `daml` following a change to `ghc`

Once you have the GHC patch you want to incorporate into the Daml repo, here's the steps you'll need to take:

1. Open a PR in the daml repo with the commit hash for the GHC patch in `ci/da-ghc-lib/compile.yml`. See [here](https://github.com/digital-asset/daml/pull/7489/commits/fedc456260f598f9924ce62d9765c3c09b8ad861)

2. Wait for CI to build `ghc-lib`/`ghc-lib-parser`, and get the new SHA from the end of the azure CI logs. The CI/azure log you are looking for is in the `Bash` subtab of the `da_ghc_lib` job. The lines of interest are at the very end of the log. See [here](https://dev.azure.com/digitalasset/adadc18a-d7df-446a-aacb-86042c1619c6/_apis/build/builds/60342/logs/52)

3. Update `stack-snapshot.yaml` with the new SHAs. See [here](https://github.com/digital-asset/daml/pull/7489/commits/f0198dc694238437357706c81b0c3d1979483d7a)

3. Run the pin command on linux or mac `bazel run @stackage-unpinned//:pin` and commit those changes as well

4. Before merging the PR, the pin command will also have to be run on windows, and those changes committed as well. You will need access to a windows machine for that: `ad-hoc.sh windows create`


### Working on an `ad-hoc` windows machine

1. First time, clone the `daml-language-ad-hoc` repo: (On following times, just pull for any updates to the scripts)
```
git clone git@github.com:DACH-NY/daml-language-ad-hoc.git
cd daml-language-ad-hoc
direnv allow
```

2. Create a new windows machine:
```
./ad-hoc.sh create windows
```

3. When the script complete, note the IP and Password.

4. Wait at least 10 minutes before trying to remotely connect to the windows machine, or else it may not inialize correctly. Really, I am not kidding. This advice is from Gary!

5. Connect to the remote desktop on the windows machine using:
```
remmina
```

6. Set the connection parameters. Best to record in a profile, so less work when reconnecting, or when making future connections:

    - set "Server" field with the _IP_.
    - set "User name" field with "u"
    - set "User password" field with the _Password_.
    - set "Resolution" to something sensible, i.e. 1400x1050
    - set "Colour depth" to "RemoteFX"

7. Ensure VPN is on. Connect using the profile, accepting the certificate.

8. On the remote desktop of the windows machine, start a command prompt. Run `bash`:
```
C:\Users\u> bash
```

9. Generate an ssh key, and add it to your github account:
```
ssh-keygen
cat .ssh/id_rsa.pub
```

10. Clone the daml repo:
```
git clone git@github.com:digital-asset/daml.git
cd daml
```

11. Run a bash script: Ignore final error: `ci/configure-bazel.sh: line 83: IS_FORK: unbound variable`
```
ci/configure-bazel.sh
```

12. Edit a powershell script: Remove all calls to the bazel function at the end of the file:
```
vi build.ps1
```

13. Run the powershell script. It uses `Scoop` to install stuff. Takes about 5 minutes. Say "yes" to a couple of popup dialogues. Then stay in `powershell`:
```
$ powershell
PS C:\Users\u\daml> .\build.ps1
```

14. Run the bazel command. That's what we came here to do! Takes 5 minutes of so.
```
bazel run @stackage-unpinned//:pin
```

15. Commit the change, and push upstream:
```
git add .\stackage_snapshot_windows.json
git config user.email "you@example.com""
git config user.name "Your Name"
git commit -m 'update snapshot after pin on windows'
git push
```

16. After confirming a successful build on CI, find the name of your windows ah-hoc machine, and destroy it:
```
./ad-hoc.sh list
./ad-hoc.sh kill <the-name-of-today's-windows-machine>
```
