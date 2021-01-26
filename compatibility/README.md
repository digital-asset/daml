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

### What is currently being tested?

We include the following SDK versions in our tests:

1. All stable SDK versions.
2. All snapshots `x.y.z-snapshot-*` for which there is no stable release `x.y.z`
3. HEAD.

Since running all tests can be rather slow, we run them in a daily
cron job. On each PR we only include HEAD and the latest stable
release.

#### Cross-version compatibility between ledger-api-test-tool

We test that the `ledger-api-test-tool` of a given version passes
against Sandbox next and classic of another version. We test all
possible version combinations here to ensure forwards and backwards
compatibility. The `ledger-api-test-tool` includes a DAR built using a
compiler from the same SDK version so this also ensures that sandbox
can load a DAR from a different SDK version. We test both in-memory
backends and postgresql backends.

Since all our JVM ledger clients use the same client libraries we
consider the `ledger-api-test-tool` to be a good proxy and if things
are not covered it should be extended.

#### Data-continuity for Sandbox classic

We have migration tests that work as follows:

1. Start the oldest Sandbox version and upload a DAR.

2. Iterate over SDK versions in order. For each version:

   1. Start sandbox of that SDK version.

   2. Run a custom Scala binary that interacts with the ledger to
      create contracts, exercise choices and query the ACS and the
      transaction service.

   3. Validates the results. This includes verifying that the
      transaction streams are the same after the migration and that the
      ACS is the same + additional test-specific checks.

We have two tests here: One that includes snapshot versions and one
that only iterates through stable versions up to HEAD. This ensures
that both individual migrations are correct as well as the migrations
from one stable version to the next work as a whole.

#### Backwards-compatibility for Daml Script

We test that the Daml Script runner from a given SDK version can load
DARs built against the Daml Script library from an older SDK. We only
guarantee backwards compatibility here.


#### Backwards-compatibility for Daml Triggers

We test that the Daml Trigger runner from a given SDK version can load
DARs built against the Daml Script library from an older SDK. We only
guarantee backwards compatibility here.

#### Backwards-compatibility for data-dependencies

We test that we can import DARs built in older SDK versions via
`data-dependencies`. We only test backwards compatibility here.

#### Cross-version compatibility for create-daml-app

We test that we can run the create-daml-app tests with JS client
libraries and codegen from one version against the JSON API and
Sandbox from another version. We test all version combinations
here. Currently we do not test different versions of the JSON API and
Sandbox. This should be covered by the `ledger-api-test-tool` tests
since the JSON API uses the same client libraries.
