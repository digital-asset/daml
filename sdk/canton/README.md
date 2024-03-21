# Canton dependency

This folder (along with the `canton` entry in `//deps.bzl`) contains the
infrastructure to work with custom Canton versions.

By default, we rely on the Canton artifact defined in `//canton_dep.bzl`.
However, we have the ability to, rather than depend on a published version,
depend on an arbitrary Canton build.

## Local development

For local development, you can set the `local` attribute to `True` in `canton_dep.bzl`, and, rather than looking at the downloaded Canton release, the Bazel build will then look for a local file under `canton/lib/canton.jar` for its Canton source.

How you get that canton jar there is entirely up to you; the assumption is that
this would be the result of building a local checkout of Canton with your own
local, uncommitted changes.

Possibly something like:

```
mkdir -p $daml_checkout/canton/lib
cd $canton_checkout
nix-shell --max-jobs 2 --run "sbt community-app/assembly"
cp community/app/target/scala-*/canton-open-source-*.jar $daml_checkout/canton/lib/canton.jar
```

Once you have things working locally, you can push your Canton branch, and
start building Daml against that even before the Canton branch gets merged.

## CI

If you want your pull request to use a custom Canton commit, rather than a
released snapshot, you can do that as follows. First, you need a Canton commit
to point to in the Canton repo (it doesn't need to be on main, any commit on
the GitHub repo can be used). Then:

- Make sure your branch sets the `local` key to `True` in `canton_dep.bzl`.
- Add a file called `arbitrary_canton_sha` to the root of the daml repository,
  that contains just the sha of the canton commit you want to build.

If the `arbitrary_canton_sha` file exists, the daml CI will clone the Canton
repo, check out the corresponding sha, build the jar, and make it available to
all three platforms. If the `local` attribute is set to `True`, just like
locally, the CI builds will use the local `canton.jar` file instead of
downloading the snapshot.

## Main branch

Generally speaking, the main branch of the repo _should_ depend on a published
snapshot rather than a specific commit. However, there may be situations where
we need to temporarily depend on a specific commit. To cater to those
situations, the daily canton bump job will reset the daml repo to depend on the
snapshot (i.e. if the tip of the main branch has an `arbitrary_canton_sha`
file, the daily job will delete it as part of the canton bump PR, and likely it
will set the `local` attibute back to `False`).

## Enterprise Edition

Some situations may require running Canton Enterprise Edition, but this is an
open-source repository so we cannot assume every contributor will have a Canton
EE license key.

Tests that require Canton EE **must** be tagged with `"canton-ee"`, which is
disabled by default through `.bazelrc`. To run those tests locally, either
explicitly target them or add `--build_tag_filters=` or `--test_tag_filters=`
as appropriate (yes, these are the full options: by setting the "running"
filters to empty for the current run, you overwrite the `-canton-ee` set in
`.bazelrc` which excludes the Canton EE tests, thereby removing the exclusion
and including the tests).

Those tests are run on CI.

If you're using a local build of canton (setting `local` to `True` per above)
_and_ you are explicitly overwriting the `*_tag_filters` to run the Canton EE
tests, they will be run using your provided `canton-ee.jar` (which therefore
needs to be an EE jar at that point).
