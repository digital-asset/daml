load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "NCURSES_SHA256",
    "NCURSES_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "ncurses",
        url = "https://ftp.gnu.org/gnu/ncurses/ncurses-{}.tar.gz".format(NCURSES_VERSION),
        sha256 = NCURSES_SHA256,
        strip_prefix = "ncurses-{}".format(NCURSES_VERSION),
        build_file = Label(":files/ncurses.BUILD.bzl"),
    )

ncurses = module_extension(implementation = _impl)
