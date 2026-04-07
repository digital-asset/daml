load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    ZIP_VERSION = "1.7.1"

    http_archive(
        name = "zip",
        build_file = ":files/haskell_zip.BUILD.bzl",
        patch_args = ["-p1"],
        patches = ["//bazel/patches:haskell/zip.patch"],
        sha256 = "0d7f02bbdf6c49e9a33d2eca4b3d7644216a213590866dafdd2b47ddd38eb746",
        strip_prefix = "zip-{}".format(ZIP_VERSION),
        urls = ["http://hackage.haskell.org/package/zip-{version}/zip-{version}.tar.gz".format(version = ZIP_VERSION)],
    )

zip = module_extension(implementation = _impl)
