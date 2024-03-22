[![Daml logo](daml-logo.png)](https://www.digitalasset.com/developers)

[![Download](https://img.shields.io/github/release/digital-asset/daml.svg?label=Download&sort=semver)](https://docs.daml.com/getting-started/installation.html)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/digital-asset/daml/blob/main/LICENSE)
[![Build](https://dev.azure.com/digitalasset/daml/_apis/build/status/digital-asset.daml?branchName=main&label=Build)](https://dev.azure.com/digitalasset/daml/_build/latest?definitionId=4&branchName=main)

Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

# Welcome to the Daml repository!

This repository hosts all code for the [Daml smart contract language and SDK](https://www.digitalasset.com/developers), originally created by
[Digital Asset](https://www.digitalasset.com). Daml is an open-source smart contract language for building future-proof distributed applications on a safe, privacy-aware runtime. The SDK is a set of tools to help you develop applications based on Daml.

## Using Daml

To download Daml, follow [the installation instructions](https://docs.daml.com/getting-started/installation.html).
Once installed, to try it out, follow the [quickstart guide](https://docs.daml.com/getting-started/quickstart.html).

If you have questions about how to use Daml or how to build Daml-based
solutions, please join us on the [Daml forum]. Alternatively, if you prefer
asking on StackOverflow, please use [the `daml` tag].

[Daml forum]: https://discuss.daml.com

[the `daml` tag]: https://stackoverflow.com/tags/daml

## Contributing to Daml

We warmly welcome [contributions](./CONTRIBUTING.md). If you are looking for ideas on how to contribute, please browse our
[issues](https://github.com/digital-asset/daml/issues). To build and test Daml:

### 1. Clone this repository

```
git clone git@github.com:digital-asset/daml.git
cd daml
```

### 2. Set up the development dependencies

Our builds require various development dependencies (e.g. Java, Bazel, Python), provided by a tool called `dev-env`.

#### Linux

On Linux `dev-env` can be installed with:

1. Install Nix by running: `bash <(curl -sSfL https://nixos.org/nix/install)`
2. Enter `dev-env` by running: `eval "$(dev-env/bin/dade assist)"`

If you don't want to enter `dev-env` manually each time using `eval "$(dev-env/bin/dade assist)"`,
you can also install [direnv](https://direnv.net). This repo already provides a `.envrc`
file, with an option to add more in a `.envrc.private` file.

#### Mac

On Mac `dev-env` can be installed with:

1. Install Nix by running: `bash <(curl -sSfL https://nixos.org/nix/install)`

    * This is a *multi-user installation* (there is no single-user installation option for macOS). Because of this, you need to configure `/etc/nix/nix.conf` to use Nix caches:

    1. Add yourself as a nix trusted user by running `echo "extra-trusted-users = $USER" | sudo tee -a /etc/nix/nix.conf`

    2. Restart the `nix-daemon` by running `sudo launchctl stop org.nixos.nix-daemon && sudo launchctl start org.nixos.nix-daemon`

2. Enter `dev-env` by running: `eval "$(dev-env/bin/dade assist)"`

If you don't want to enter `dev-env` manually each time using `eval "$(dev-env/bin/dade assist)"`,
you can also install [direnv](https://direnv.net). This repo already provides a `.envrc`
file, with an option to add more in a `.envrc.private` file.

Note that after a macOS update it can appear as if Nix is not installed. This is because macOS updates can modify shell config files in `/etc`, which the multi-user installation of Nix modifies as well. A workaround for this problem is to add the following to your shell config file in your `$HOME` directory:

```
# Nix
if [ -e '/nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh' ]; then
  . '/nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh'
fi
# End Nix
```
See https://github.com/NixOS/nix/issues/3616 for more information about this issue.

##### MacOS M1

The above procedure will use and build native arm64 M1 binaries for this
project. However, note that at the time of writing the CI system of the Daml
project does not yet include MacOS M1 nodes. Therefore, the M1 configuration is
untested on CI, and the remote cache is not populated with native M1 artifacts.

If you encounter issues with a native M1 build, then you can configure project
to build x86-64 binaries instead and run them through Rosetta. To do that
replace the contents of the file `nix/system.nix` with the following content:

```nix
"x86_64-darwin"
```

#### Windows

On Windows you need to enable long file paths by running the following command in an admin powershell:

```
Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' -Name LongPathsEnabled -Type DWord -Value 1
```

You also need to configure Bazel for Windows:

```
echo "build --config windows" > .bazelrc.local
```

Note, if you are on a Windows ad-hoc or CI machine you can use
`ci/configure-bazel.sh` instead of performing these steps manually.
In that case, you should checkout the `daml` repository into the path
`D:\a\1\s` in order to be able to use remote cache artifacts.

Then start `dev-env` from PowerShell with:

```
.\dev-env\windows\bin\dadew.ps1 install
.\dev-env\windows\bin\dadew.ps1 sync
.\dev-env\windows\bin\dadew.ps1 enable
```

In all new PowerShell processes started, you need to repeat the `enable` step.

### 3. Lint, build, and test

We have a single script to build most targets and run the tests. On Linux and Mac run `./build.sh`. On Windows run `.\build.ps1`. Note that these scripts may take over an hour the first time.

To just build do `bazel build //...`, and to just test do `bazel test //...`. To read more about Bazel and how to use it, see [the Bazel site](https://bazel.build).

On Mac if building is causing trouble complaining about missing nix packages, you can try first running `nix-build -A tools -A cached nix` repeatedly until it completes without error.

CI will run a few checks with regards to formatting, linting, presence of copyright headers, and so on. In order to make sure your PR can smoothly go through those checks, we use
a tool called [`pre-commit`](https://pre-commit.com/). The tool is managed by Nix and you don't have to install it. If you use `direnv`, the tool will automatically install a `pre-push`
hook that will run the relevant checks right before you push. This will give you a chance to apply necessary amendments before your contribution reaches CI. If you don't use `direnv` you
can still use the tool by activating it manually (have a look at how it's done in `.envrc`). If you use `direnv` but prefer to not use the tool at all, you can add the line
`export DADE_NO_PRE_COMMIT=anything_really` to `.envrc.private`. You can also customize the phase at which the hooks will run by exporting the environment variable `DADE_PRE_COMMIT_HOOK_TYPE`
and setting it to one of the supported stages (`pre-commit` is a common choice, but the default when installed via `direnv` will be `pre-push` to reduce the times the hooks will run while
still making sure that you can have a tight feedback loop to fix linting errors).

### 4. Installing a local copy

On Linux and Mac run `daml-sdk-head` which installs a version of the SDK with version number `0.0.0`. Set the `version:` field in any Daml project to 0.0.0 and it will use the locally installed one.

On Windows:

```
bazel build //release:sdk-release-tarball
tar -vxf .\bazel-bin\release\sdk-release-tarball-ce.tar.gz
cd sdk-*
daml\daml.exe install . --install-assistant=yes
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
