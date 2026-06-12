load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:zip.version.bzl",
    "ZIP_SHA256",
    "ZIP_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "zip",
        build_file = ":files/zip.BUILD.bzl",
        patch_args = ["-p1"],
        patches = ["//bazel/patches:haskell/zip.patch"],
        sha256 = ZIP_SHA256,
        strip_prefix = "zip-{}".format(ZIP_VERSION),
        urls = ["http://hackage.haskell.org/package/zip-{version}/zip-{version}.tar.gz".format(version = ZIP_VERSION)],
    )

zip = module_extension(implementation = _impl)
