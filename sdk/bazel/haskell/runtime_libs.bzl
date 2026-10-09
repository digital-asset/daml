load("@os_info//:os_info.bzl", "is_darwin", "is_windows")

_LIB_PATH_VAR = "DYLD_LIBRARY_PATH" if is_darwin else "LD_LIBRARY_PATH"

_TINFO = "//bazel/haskell/toolchain:libtinfo.so.6"

CBITS_DLL = "@grpc_haskell_core_cbits//:merged_cbits"

def runtime_lib_path_export(lib_dirs):
    var = "PATH" if is_windows else _LIB_PATH_VAR
    return 'export {var}="{dirs}:${{{var}:-}}"'.format(
        var = var,
        dirs = ":".join(["$PWD/" + d for d in lib_dirs]),
    )

DAMLC_RUNTIME_LIB_DIR_DEPS = [
    Label(CBITS_DLL),
] if is_windows else [
    Label("//bazel/haskell/toolchain:tinfo_libs"),
    Label("@libz//:libs"),
    Label("@gmp//:libs"),
    Label("@bzip2//:libs"),
]

DAMLC_RUNTIME_LIBS = [CBITS_DLL] if is_windows else [
    "@bzip2//:libs",
    "@gmp//:libs",
    "@libz//:libs",
] + ([] if is_darwin else [_TINFO])

DAMLC_RUNTIME_LIB_PATH_EXPORT = 'export PATH="$$(dirname $(location {})):$${{PATH:-}}"'.format(CBITS_DLL) if is_windows else 'export {var}="{dirs}:$${{{var}:-}}"'.format(
    var = _LIB_PATH_VAR,
    dirs = ":".join([
        "$$(dirname $(location @libz//:libs))",
        "$$(dirname $$(set -- $(locations @gmp//:libs); echo $$1))",
        "$$(dirname $(location @bzip2//:libs))",
    ] + ([] if is_darwin else ["$$(dirname $(location {}))".format(_TINFO)])),
)
