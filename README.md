[![DAML logo](daml-logo.png)](https://www.daml.com)

[![Download](https://img.shields.io/github/release/digital-asset/daml.svg?label=Download)](https://docs.daml.com/getting-started/installation.html)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/digital-asset/daml/blob/main/LICENSE)
[![Build](https://dev.azure.com/digitalasset/daml/_apis/build/status/digital-asset.daml?branchName=main&label=Build)](https://dev.azure.com/digitalasset/daml/_build/latest?definitionId=4&branchName=main)

Copyright 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

# Welcome to the DAML repository!

This repository hosts all code for the [DAML smart contract language and SDK](https://daml.com/), originally created by
[Digital Asset](https://www.digitalasset.com). DAML is an open-source smart contract language for building future-proof distributed applications on a safe, privacy-aware runtime. The SDK is a set of tools to help you develop applications based on DAML.

## Using DAML

To download DAML, follow [the installation instructions](https://docs.daml.com/getting-started/installation.html).
Once installed, to try it out, follow the [quickstart guide](https://docs.daml.com/getting-started/quickstart.html).

If you have questions about how to use DAML or how to build DAML-based solutions, please ask them on
[StackOverflow using the `daml` tag](https://stackoverflow.com/tags/daml).

## Contributing to DAML

We warmly welcome [contributions](./CONTRIBUTING.md). If you are looking for ideas on how to contribute, please browse our
[issues](https://github.com/digital-asset/daml/issues). To build and test DAML:

### 1. Clone this repository

```
git clone git@github.com:digital-asset/daml.git
cd daml
```

### 2. Set up the development dependencies

Our builds require various development dependencies (e.g. Java, Bazel, Python), provided by a tool called `dev-env`.

#### Linux and Mac

On Linux and Mac `dev-env` can be installed with:

1. Install Nix by running: `bash <(curl -sSfL https://nixos.org/nix/install)`
2. Enter `dev-env` by running: `eval "$(dev-env/bin/dade assist)"`

If you don't want to enter `dev-env` manually each time using `eval "$(dev-env/bin/dade assist)"`,
you can also install [direnv](https://direnv.net). This repo already provides a `.envrc`
file, with an option to add more in a `.envrc.private` file.

#### Windows

On Windows you need to enable long file paths by running the following command in an admin powershell:

```
Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' -Name LongPathsEnabled -Type DWord -Value 1
```

Then start `dev-env` from PowerShell with:

```
.\dev-env\windows\bin\dadew.ps1 install
.\dev-env\windows\bin\dadew.ps1 sync
.\dev-env\windows\bin\dadew.ps1 enable
```

In all new PowerShell processes started, you need to repeat the `enable` step.

### 3. First build and test

We have a single script to build most targets and run the tests. On Linux and Mac run `./build.sh`. On Windows run `.\build.ps1`. Note that these scripts may take over an hour the first time.

To just build do `bazel build //...`, and to just test do `bazel test //...`. To read more about Bazel and how to use it, see [the Bazel site](https://bazel.build).

On Mac if building is causing trouble complaining about missing nix packages, you can try first running `nix-build -A tools -A cached nix` repeatedly until it completes without error.

### 4. Installing a local copy

On Linux and Mac run `daml-sdk-head` which installs a version of the SDK with version number `0.0.0`. Set the `version:` field in any DAML project to 0.0.0 and it will use the locally installed one.

On Windows:

```
bazel build //release:sdk-release-tarball
tar -vxf .\bazel-bin\release\sdk-release-tarball.tar.gz
cd sdk-*
daml\daml.exe install . --activate
```

That should tell you what to put in the path, something along the lines of `C:\Users\admin\AppData\Roaming\daml\bin`.
Note that the Windows build is not yet fully functional.

### Caching: build speed and disk space considerations

Bazel has a lot of nice properties, but they come at the cost of frequently rebuilding "the world".
To make that bearable, we make extensive use of caching. Most artifacts should be cached in our CDN,
which is configured in `.bazelrc` in this project.

However, even then, you may end up spending a lot of time (and bandwidth!) downloading artifacts from
the CDN. To alleviate that, by default, our build will create a subfolder `.bazel-cache` in this
project and keep an on-disk cache. **This can take about 10GB** at the time of writing.

To disable the disk cache, remove the following lines:

```
build:linux --disk_cache=.bazel-cache
build:darwin --disk_cache=.bazel-cache
```

from the `.bazelrc` file.

If you work with multiple copies of this repository, you can point all of them to the same disk cache
by overwriting these configs in either a `.bazelrc.local` file in each copy, or a `~/.bazelrc` file
in your home directory.

### Shared memory segment issues

On macOS at least, it looks like our setup does not always properly close the
resources PostgreSQL uses. After a number of test runs, you may encounter an
error message along the lines of:

```
FATAL:  could not create shared memory segment: No space left on device
DETAIL:  Failed system call was shmget(key=5432001, size=56, 03600).
HINT:  This error does *not* mean that you have run out of disk space. It occurs either if all available shared memory IDs have been taken, in which case you need to raise the SHMMNI parameter in your kernel, or because the system's overall limit for shared memory has been reached.
        The PostgreSQL documentation contains more information about shared memory configuration.
child process exited with exit code 1
```

In this case, this is a memory leak, so increasing `SHMNI` (or `SHMALL` etc.)
as suggested will only delay the issue. You can look at the existing shared
memory segments on your system by running `ipcs -mcopt`; this will print a line
per segment, indicating the process ID of the last process to connect to the
segment as well as the last access time and the number of currently connected
processes.

If you identify segments with no connected processes, and you are confident you
can remove them, you can do so with `ipcrm $sid`, where `$sid` is the process
ID displayed (as the second column) by `ipcs`. Not many macOS applications use
shared memory segments; **if you have verified that all the existing memory
segments on your machine need to be deleted**, e.g. because they have all been
created by PostgreSQL instances that are no longer running, here is a Bash
invocation you can use to remove all shared memory segments from your system.

**This is a dangerous command. Make sure you understand what it does before
running it.**

```
for shmid in $(ipcs -m | sed 1,3d | awk '{print $2}' | sed '$d'); do ipcrm -m $shmid; done
```


### Haskell profiling builds

To build Haskell executables with profiling enabled, pass `-c dbg` to
Bazel, e.g. `bazel build -c dbg damlc`. If you want to build the whole
SDK with profiling enabled use `daml-sdk-head --profiling`.
