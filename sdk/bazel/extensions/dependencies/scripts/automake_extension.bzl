load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    http_archive(
        name = "automake",
        urls = ["https://ftp.gnu.org/gnu/automake/automake-1.16.5.tar.gz"],
        strip_prefix = "automake-1.16.5",
        sha256 = "07bd24ad08a64bc17250ce09ec56e921d6343903943e99ccf63bbf0705e34605",
        build_file = ":files/automake.BUILD.bzl",
    )

automake = module_extension(implementation = _impl)
