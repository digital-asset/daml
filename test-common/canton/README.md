# Canton dependency

This folder contains the  infrastructure to work with custom 
Canton Enterprise Edition versions.

By default, we rely on the Canton-EE artifact defined in
`BUILD.bazel`.  However, we have the ability to, rather than depend on
a published version, depend on an arbitrary jar.

## Local development

For local development, you can set the `local_ee_canton` variable in
`BUILD.bazel`, and, rather than looking at the downloaded Canton-EE
release, the Bazel build will then look for a local file under
`canton/lib/canton-ee.jar` for its Canton source.

How you get that canton-ee jar there is entirely up to you; the assumption is
that this would be the result of building a local checkout of Canton with your
own local, uncommitted changes.

Once you have things working locally, you can push your Canton branch, and
start building Daml against that even before the Canton branch gets merged.

## Running Enterprise Edition Tests

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
