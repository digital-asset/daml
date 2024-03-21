[![Daml logo](daml-logo.png)](https://www.digitalasset.com/developers)

[![Download](https://img.shields.io/github/release/digital-asset/daml.svg?label=Download&sort=semver)](https://docs.daml.com/getting-started/installation.html)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/digital-asset/daml/blob/main/LICENSE)
[![Build](https://dev.azure.com/digitalasset/daml/_apis/build/status/digital-asset.daml?branchName=main&label=Build)](https://dev.azure.com/digitalasset/daml/_build/latest?definitionId=4&branchName=main)

Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

# Build using Apple Mac OS on M1

## Pre-requisites

```
brew install packer
brew install cirruslabs/cli/tart
```

## initial Base Image
```
tart create --from-ipsw=latest monterey-base
tart run monterey-base
```
Follow steps under create from Scratch: https://github.com/cirruslabs/tart

## Build Node using Packer

```
packer build -only=init-1.tart-cli.init-1 daml-build.pkr.hcl
packer build -only=init-2.tart-cli.init-2 daml-build.pkr.hcl
packer build -only=init-3.tart-cli.init-3 daml-build.pkr.hcl
```



