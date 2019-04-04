# Extractor

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

1. Create a branch `extractor-release-<major>-<minor>-<point>` for the release. Check the file [ledger-tools/extractor/version.sbt](version.sbt). For example, let's say the the current
version of the project is `0.9.3-SNAPSHOT`. You need to create a branch called `extractor-release-0-9-3`;
2. Move the ## Unreleased section of [ledger-tools/extractor/UNRELEASED.md](UNRELEASED.md) to the new release
version you have created in [ledger-tools/extractor/CHANGELOG.md](CHANGELOG.md) and commit to that branch. The message of the commit is not important as it
will be discarded;
3. Run `sbt release `. This will ask for the release version and the next version, and will create
commit with the release version and next version, and also takes care about tagging;
4. Push your **branch and tag**:

```
git push origin release/extractor/0.9.3    # check the tag that has been created, and push it
git push -u origin extractor-release-0-9-3 # push your branch
```
5. Go to the release [Jenkins job](http://ci.da-int.net/job/ledger-tools/job/extractor-release/build?delay=0sec)
Enter the tag you published and run the job.
6. Create a Pull Request from your branch, have it reviewed
and merged. After it's done, you can delete the branch.