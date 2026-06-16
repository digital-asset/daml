load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "NCURSES_SHA256",
    "NCURSES_VERSION",
)

# Download-only ncurses source. libtinfo is built hermetically at action time
# via //bazel/native:tinfo (configure_make on the cc toolchain), not at
# repo-fetch time, so no host cc is required.
def _impl(_module_ctx):
    http_archive(
        name = "ncurses",
        url = "https://ftp.gnu.org/gnu/ncurses/ncurses-{}.tar.gz".format(NCURSES_VERSION),
        sha256 = NCURSES_SHA256,
        strip_prefix = "ncurses-{}".format(NCURSES_VERSION),
        build_file = "//bazel/native:files/ncurses.BUILD.bzl",
    )

ncurses = module_extension(implementation = _impl)
