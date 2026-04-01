load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    http_archive(
        name = "m4",
        urls = ["https://ftp.gnu.org/gnu/m4/m4-1.4.19.tar.gz"],
        strip_prefix = "m4-1.4.19",
        sha256 = "3be4a26d825ffdfda52a56fc43246456989a3630093cced3fbddf4771ee58a70",
        build_file = ":files/m4.BUILD.bzl",
    )

m4 = module_extension(implementation = _impl)
