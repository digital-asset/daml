load("@os_info//:os_info.bzl", "is_darwin")

_LIB_PATH_VAR = "DYLD_LIBRARY_PATH" if is_darwin else "LD_LIBRARY_PATH"

_TINFO = "//bazel/haskell/toolchain:libtinfo.so.6"

DAMLC_RUNTIME_LIBS = [
    "@bzip2//:libs",
    "@gmp//:libs",
    "@libz//:libs",
] + ([] if is_darwin else [_TINFO])

DAMLC_RUNTIME_LIB_PATH_EXPORT = 'export {var}="{dirs}:$${{{var}:-}}"'.format(
    var = _LIB_PATH_VAR,
    dirs = ":".join([
        "$$(dirname $(location @libz//:libs))",
        "$$(dirname $$(set -- $(locations @gmp//:libs); echo $$1))",
        "$$(dirname $(location @bzip2//:libs))",
    ] + ([] if is_darwin else ["$$(dirname $(location {}))".format(_TINFO)])),
)
