# Tools manifests files

DADEW uses [Scoop][scoop] for tools provisioning. Tools manifest files are in fact Scoop's App manifest files and are documented [here][scoop-manifests].

Tool name mentioned in `.dadew` file is the name of the manifest file (excluding it's `.json` extention).

## Adding new tool

To add new tool:
- create new manifest file under `/dev-env/windows/manifests/` folder, ensuring:
    - it follows the naming convention of `<name>-<version>.json`
    - it sources binaries from a URL, which points to specific version of the tool and is unlikely to change
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

[scoop]: https://github.com/lukesampson/scoop
[scoop-manifests]: https://github.com/lukesampson/scoop/wiki/App-Manifests
[scoop-bucket]: https://github.com/lukesampson/scoop/tree/master/bucket
[scoop-all-buckets]: https://github.com/lukesampson/scoop/blob/master/buckets.json
