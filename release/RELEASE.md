# Making a Release

1. Update https://github.com/digital-asset/daml/docs/support/release-notes.rst
   by adding a new header for the new version above any changes since the last
   version.
1. Make a PR that only bumps the version number in the VERSION
   file. It is important that the PR only changes the VERSION file.
1. Merge the PR using a rebase merge.
1. Once CI has passed for the merged PR, the release should be
   available on bintray.
1. Activate the new version with `da use VERSION`. Note that it will
   not be picked up by `da upgrade` at this point.
1. Run through the manual test plan described in https://docs.google.com/document/d/16amcy7bQodXSHjEmKhAUiaPf6O92gUbch1OyixDEvSM/edit?ts=5ca5be00.
1. If it passes the release should be made public. This currently
   consists of three steps.

   1. Tag the release as `visible-external` on Bintray. This step can
      only be done by someone with permissions to set tags on Bintray.
      After this step the release will be picked up by `da upgrade`.

   1. Trigger the CircleCI jobs to create the Docker images. To do so,
      go to
      https://circleci.com/gh/DACH-NY/workflows/damlc-docker/tree/master
      and click "rerun" on "master / Main Variant" and on "master / CircleCI Variant".
      Once the jobs have passed, you should see two new images on https://hub.docker.com/r/digitalasset/daml-sdk/tags.

   1. Tag the commit and push the tag:
      ```
      git checkout master
      # Checkout the release commit, you can use the following to
      # find it assuming the commit message contains "release".
      git log --grep=release -i
      git checkout SHA_OF_RELEASE_COMMIT
      git tag -a v0.11.31 -m "SDK 0.11.31" # Change the version number
      git push origin v0.11.31
      ```
      Verify that you see the tag at https://github.com/digital-asset/daml/releases.

