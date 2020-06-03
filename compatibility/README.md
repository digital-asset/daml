This directory contains the infrastructure and test setup for
cross-version compatibility testing. We make this a separate Bazel
workspace to make it easier to enforce that we only depend on release
artifacts.

### How to

Before using this, make sure to always run the scripts that build the
SDK, namely `./build-release-artifacts.sh` and `./build-ts-libs.sh`.
These should be run every time there is a relevant change you want
to be there for this workspace to use.

Any Bazel command can then be used afterwards. `./test.sh` are the
tests run on CI.

