# Making a Release

1. Make a PR that bumps the version number in the `VERSION`
   file and adds a new header for the new version in
   `docs/source/support/release-notes.rst`.
   It is important that the PR only changes `VERSION` and `release-notes.rst`.
1. "Squash and merge" the PR.
1. Once CI has passed for the corresponding master build, the release should be
   available on bintray and GitHub, as well as properly tagged.
1. Activate the new version with `da use VERSION`. Note that it will
   not be picked up by `da upgrade` at this point.
1. Run through the manual test plan described in https://docs.google.com/document/d/16amcy7bQodXSHjEmKhAUiaPf6O92gUbch1OyixDEvSM/edit?ts=5ca5be00.
1. If it passes, the release should be made public. This currently
   consists of three steps:

   1. Tag the release as `visible-external` on Bintray. This step can
      only be done by someone with permissions to set tags on Bintray.
      After this step the release will be picked up by `da upgrade`.

   1. Trigger the CircleCI jobs to create the Docker images. To do so,
      go to
      https://circleci.com/gh/DACH-NY/workflows/damlc-docker/tree/master
      and click "rerun" on "master / Main Variant" and on "master / CircleCI Variant".
      Once the jobs have passed, you should see two new images on https://hub.docker.com/r/digitalasset/daml-sdk/tags.

   1. Publish the draft release on GitHub by going to [the releases
      page](https://github.com/digital-asset/daml/releases) and clicking the
      Edit button for the relevant release.
