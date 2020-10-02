# DAML-LF archive

This component contains the `.proto` definitions specifying the format
in which DAML-LF packages are stored -- the DAML-LF archive. All the
proto definitions are kept in the directory
`src/protobuf/com/daml/daml_lf_dev/`

The entry point definition is `Archive` in
`src/protobuf/com/daml/daml_lf_dev/daml_lf.proto`.  `Archive`
contains some metadata about the actual archive (currently the hashing
function and the hash), and then a binary blob containing the
archive. The binary blob must be an `ArchivePayload` -- we keep it in
binary form to facilitate hashing, signing, etc. The encoding and
decoding of the payload is handled by Haskell and Java libraries in
`daml-core-package`, so that consumers and producers do not really
need to worry about it.

`ArchivePayload` is a sum type containing the various DAML-LF versions
supported by the DAML-LF archive. Currently we have two major versions:

* `DAML-LF-0`, which is the deprecated legacy DAML core;
* `DAML-LF-1`, which is the first version of DAML-LF as specified by
    <https://github.com/digital-asset/daml/blob/master/daml-lf/spec/daml-lf-1.rst>.

## Snapshot versions

The component contains also an arbitrary number of snapshots of the
protobuf definitions as they were as the time a particular version of
DAML-LF was frozen. For versions <= 1.8, those snapshots are kept in the directories
`src/protobuf/com/digitalasset/daml_lf_x_y/`, where `x.y` is a
already frozen DAML-LF version.  For newer versions, the directory is
`src/protobuf/com/daml/daml_lf_x_y/`. A snapshot for version `x.y` can be
used to read any DAML-LF version from `1.0` to `x.y` without suffering
breaking changes (at the generated code level) often introduced in the
current version.

## Building

It produces several libraries containing code to encode / decode such
definition, a Haskell one, and several Java ones:

```
$ bazel build //daml-lf/archive:daml_lf_archive_haskell_proto
$ bazel build //daml-lf/archive:daml_lf_dev_archive_java_proto
$ bazel build //daml-lf/archive:daml_lf_1_6_archive_java_proto
```

## Editing the `.proto` definitions

When editing the proto definitions, **you must make sure to not change
them in a backwards-incompatible way**. To make sure this doesn't happen:

* **DO NOT** delete message fields;
* **DO NOT** change the number of a message field or an enum value;
* **DO NOT** change the type of a message field;

Note that "fields" include `oneof` fields. Also note that the "don't
delete fields" rule is there not because they introduce a backwards
incompatible change, but rather because after a field has been deleted
another commiter might redefine it with a different type without
realizing.

What is OK is renaming message fields while keeping the number and semantics unchanged.
For example, if you have

```
message Foo {
  bytes blah = 1;
}
```

it's OK to change it to

```
message Foo {
  // this field is deprecated -- use baz instead!
  bytes blah_deprecated = 1;
  string baz = 2;
}
```

## Conversion from the `.proto` to AST

The `.proto` definitions contain the serialized format for DAML-LF
packages, however the code to convert from the `.proto` definitions to
the actual AST lives elsewhere.


