[![DAML logo](daml-logo.png)](https://www.daml.com)

Copyright 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

# Welcome to the DAML repository!

This repository hosts all code for the DAML smart contract language, originally created by
[Digital Asset](https://www.digitalasset.com).

## To start using DAML

To download DAML, follow [the installation instructions on docs.daml.com](https://docs.daml.com/getting-started/installation.html).
To try out using it, follow the [quickstart guide](https://docs.daml.com/getting-started/quickstart.html).

If you have questions about how to use DAML or how to build DAML-based solutions, please ask
them on [StackOverflow using the `daml` tag](https://stackoverflow.com/tags/daml).

## To start contributing to DAML

We warmly welcome [contributions](./CONTRIBUTING.md).

### 1. Clone this repository

`git clone git@github.com:digital-asset/daml.git`.

### 2. Set up the DA Development Environment ("`dev-env`")

`dev-env` provides dependencies required during the build phase, like Java, Bazel, and Python
for some tooling scripts. The code itself is built using Bazel.

#### Set up `dev-env` on Linux or macOS

1. Install Nix: `bash <(curl https://nixos.org/nix/install)`
2. Enter `dev-env`: `eval "$(dev-env/bin/dade assist)"`

If you don't want to enter `dev-env` manually each time using `eval "$(dev-env/bin/dade assist)"`,
you can also install [direnv](https://direnv.net). This repo already provides a `.envrc`
file, with an option to add more in a `.envrc.private` file.

#### Set up `dev-env` on Windows

We're working on Windows support (for both end users and developers), but it's not ready yet.
[Sign up](https://hub.daml.com/sdk/windows) to be notified when it is available.

### 3. Build the source code

Run `bazel build //...`

This builds the code, and will likely take an hour or more.

Now you've built, rebuilding the code after a change will be much faster because Bazel caches
unchanged build artefacts. To read more about Bazel and how to use it, see [the Bazel site](https://bazel.build).

To run the tests, run `bazel test //...`

### 4. Contribute!

If you are looking for ideas on how to contribute, please browse our
[issues](https://github.com/digital-asset/daml/issues).

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
