# Tools manifests files

DADEW uses [Scoop][scoop] for tools provisioning. Tools manifest files are in fact Scoop's App manifest files and are documented [here][scoop-manifests].

Tool name mentioned in `.dadew` file is the name of the manifest file (excluding it's `.json` extention).

## Adding new tool

To add new tool:
- create new manifest file under `/dev-env/windows/manifests/` folder, ensuring:
    - it follows the naming convention of `<name>-<version>.json`
    - it sources binaries from a URL, which points to specific version of the tool and is unlikely to change
    (preferably source it from the engineering host as described below)
- add a `<name>-<version>` entry to `/.dadew` file, extending `tools` element list

## Adding new version of the existing tool 

Process of adding new version of the existing tool is very similar to adding new one, but:
- you should not modify existing manifest files to allow backward compatibility of win-dev-env,
- you should not remove the old tool's manifest file
- you should replace the existing entry in the `/.dadew` file with the new one

## Updating the existing version of a tool

In some cases there is a need to update the existing manifest file, for example to introduce an environment variable or
update the binary minor release version. In such case manifest file can be changed in-place. `dadew` will detect such change
and perform the tool re-installation on `dadew sync` call automatically.

## Source of manifests

Default set of Scoop App manifests (also called a default bucket) can be found [here][scoop-bucket].

Other buckets are listed in Scoop's `buckets.json` [file][scoop-all-buckets].

## Source of binaries

All binaries referenced in manifest files should be hosted internally to reduce the likelihood of an upstream change.


In general, binaries are provided from: https://engineering.da-int.net/nix-vendored/<tool_name>/<file_name>


which is kept in-sync with the upstream Artifactory `nix-vendored` repository:
https://digitalasset.jfrog.io/digitalasset/webapp/#/artifacts/browse/tree/General/nix-vendored

To upload a binary to the repo follow the link above and click 'Deploy' button on the page.
Ensure that:
- the Target Repository is `nix-vendored`,
- the Target Path attribute is in form of `/<tool_name>/<file_name>`, e.g. `/bazel/bazel-0.20.0-windows-x86_64.zip`,
- the `<file_name>` contains the tool version (and architecture label if required) like the one above

To read more on how to deploy artifacts to the Artifactory see [JFrog documentation][jfrog-deploying].

[jfrog-deploying]: https://www.jfrog.com/confluence/display/RTF/Deploying+Artifacts
[scoop]: https://github.com/lukesampson/scoop
[scoop-manifests]: https://github.com/lukesampson/scoop/wiki/App-Manifests
[scoop-bucket]: https://github.com/lukesampson/scoop/tree/master/bucket
[scoop-all-buckets]: https://github.com/lukesampson/scoop/blob/master/buckets.json