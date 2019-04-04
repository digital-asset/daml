# Code generation for tests

Running:

```bash
$ bash generate-test-types.sh
```

from inside this directory will result in `./TestProto.hs` and
`./TestProtoImport.hs` being regenerated from `../test-files/test_proto.proto`
and `../test-files/test_proto_import.proto`, respectively.

We'll eventually `nix`-ify the building of codegen artifacts, so this is a bit
of a stopgap.
