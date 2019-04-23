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
The [Ledger API Introduction](https://docs.daml.com/app-dev/ledger-api-introduction/index.html) contains introductory material as well as links to the protodocs reference documentation.

See [the docs README)[/docs/README.md] for more about how to preview and publish documentation.

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

1. Create a branch `ledger-api-release-<major>-<minor>-<point>` for the release. Check the file [ledger-api/version.sbt](version.sbt). For example, let's say the the current
version of the project is `0.9.3-SNAPSHOT`. You need to create a branch called `ledger-api-release-0-9-3`;
2. Bump the version for the NPM distribution channel:

      (cd npm-package && npm version <major>.<minor>.<point>)

3. Move the ## Unreleased section of [ledger-api/UNRELEASED.md](UNRELEASED.md) to the new release
version you have created in [ledger-api/CHANGELOG.md](CHANGELOG.md) and commit to that branch. The message of the commit is not important as it
will be discarded;
4. Run `sbt release`. This will ask for the release version and the next version, and will create
commit with the release version and next version, and also takes care about tagging;
5. Push your **branch and tag**:

```
git push origin release/ledger-api/0.9.3        # check the tag that has been created, and push it
git push -u origin ledger-api-release-0-9-3    # push your branch
```
6. Go to the release [Jenkins job](http://ci.da-int.net/job/ledger-api/job/release/build?delay=0sec)
Enter the tag you published and run the job.
7. Create a Pull Request from your branch, have it reviewed
and merged. After it's done, you can delete the branch.
