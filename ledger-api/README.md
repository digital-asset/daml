# Ledger-API

This is the API code for the ledger, which contains:
 * gRPC API definitions
 * Generated Scala bindings
 * gRPC-RS bridge
 * gRPC-Akka bridge
 * Server API classes with validation
 * Prototype Server
 * Prototype Clients
   * Scala
   * Python
   * Node.js
 * Integration tests for all of the above

# Documentation
The [Ledger API Introduction](docs/ledger-api-introduction) documentation package contains introductory material as well as the protodocs reference documentation.

**Generate ProtoDocs** with `ledger-api/docs/ledger-api-introduction/generate-protodocs` script.

**Preview** the documentation with `da-docs-preview` or `da-docs-preview-all`.

**Publish** the documentation with `da-docs-publish x.y.z`.

# Build

The code uses SBT as a build system

```
sbt compile
```

To run the tests

```
sbt test
```

# Release

The release process on jenkins also publishes the [Ledger API Introduction](docs/ledger-api-introduction) documentation package with the release version.

1. Create a branch `ledger-api-release-<major>-<minor>-<point>` for the release. Check the file [ledger-api/version.sbt](version.sbt). For example, let's say the the current
version of the project is `0.9.3-SNAPSHOT`. You need to create a branch called `ledger-api-release-0-9-3`;
2. Generate the protobuf ProtoDocs with `ledger-api/docs/ledger-api-introduction/generate-protodocs` and commit to that branch.
3. Bump the version for the NPM distribution channel:

      (cd npm-package && npm version <major>.<minor>.<point>)

4. Move the ## Unreleased section of [ledger-api/UNRELEASED.md](UNRELEASED.md) to the new release
version you have created in [ledger-api/CHANGELOG.md](CHANGELOG.md) and commit to that branch. The message of the commit is not important as it
will be discarded;
5. Run `sbt release`. This will ask for the release version and the next version, and will create
commit with the release version and next version, and also takes care about tagging;
6. Push your **branch and tag**:

```
git push origin release/ledger-api/0.9.3        # check the tag that has been created, and push it
git push -u origin ledger-api-release-0-9-3    # push your branch
```
7. Go to the release [Jenkins job](http://ci.da-int.net/job/ledger-api/job/release/build?delay=0sec)
Enter the tag you published and run the job.
8. Create a Pull Request from your branch, have it reviewed
and merged. After it's done, you can delete the branch.
