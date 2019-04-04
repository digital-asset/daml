[![DAML logo](daml-logo.png)](https://www.daml.com)

Copyright 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

# Welcome to the DAML repository!

This repository hosts all code for the DAML smart contract language, originally created by [Digital Asset](https://www.digitalasset.com).

## To start using DAML

See our documentation at [docs.daml.com](https://docs.daml.com).

Download the DAML SDK Developer Preview at [https://www.daml.com](https://www.daml.com).

If you have questions about how to use DAML or how to build DAML-based solutions, please ask them on [StackOverflow using the `daml` tag](https://stackoverflow.com/tags/daml).

## To start contributing to DAML

We warmly welcome [contributions](./CONTRIBUTING.md).

### Cloning this repository
`git clone git@github.com:digital-asset/daml.git`.

### Setting up the DA Development Environment (also known as `dev-env`)
dev-env is used to provide dependencies required during build phase, like java,
bazel, and python for some tooling scripts, while the code itself is built through
bazel.

#### Linux and macOS
1. Install Nix: `bash <(curl https://nixos.org/nix/install)`
2. Enter dev-env: `eval "$(dev-env/bin/dade assist)"`

To avoid entering dev-env manually each time through `eval "$(dev-env/bin/dade
assist)"`, you can also install [direnv](https://direnv.net).

This repo already provides a `.envrc` file, with an option to add more in a
`.envrc.private` file.

#### Windows
We're working on Windows support (both users and developers), but it's not ready yet. [Sign up](https://hub.daml.com/sdk/windows) to be notified when it is available.

### Build the source code
`bazel build //...`. This will likely take an hour or more.

You have now built the code and run the tests. Rebuilding the code after a change will be significantly faster because Bazel caches unchanged build artefacts. You can read more about Bazel and how to use it [here](https://bazel.build).

4. Run the tests: `bazel test //...`

If you are looking for ideas on how to contribute, please browse our [issues](https://github.com/digital-asset/daml/issues).

### Caching: build speed and disk space considerations

Bazel has a lot of nice properties, but they come at the cost of frequently rebuilding "the world". To make that bearable, we make extensive use of caching. Most artifacts should be cached in our CDN, which is configured in `.bazelrc` in this project.

However, even then, you may end up spending a lot of time (and bandwidth!) downloading artifacts from the CDN. To alleviate that, by default, our build will create a subfolder `.bazel-cache` in this project and keep an on-disk cache. **This can take about 10GB** at the time of writing.

To disable the disk cache, simply remove the following lines:

```
build:linux --disk_cache=.bazel-cache
build:darwin --disk_cache=.bazel-cache
```

from the `.bazelrc` file.

If you work with multiple copies of this repository, you can point all of them to the same disk cache by overwriting these configs in either a `.bazelrc.local` file in each copy, or a `~/.bazelrc` file in your home directory.
