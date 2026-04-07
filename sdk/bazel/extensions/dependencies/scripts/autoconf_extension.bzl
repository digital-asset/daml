load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    http_archive(
        name = "autoconf",
        urls = ["https://ftp.gnu.org/gnu/autoconf/autoconf-2.71.tar.gz"],
        strip_prefix = "autoconf-2.71",
        sha256 = "431075ad0bf529ef13cb41e9042c542381103e80015686222b8a9d4abef42a1c",  # fill after first download
        build_file = ":files/autoconf.BUILD.bzl",
    )

autoconf = module_extension(implementation = _impl)
