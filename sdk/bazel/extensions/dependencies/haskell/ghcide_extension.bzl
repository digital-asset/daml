load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:ghcide.version.bzl",
    "GHCIDE_REPO_URL",
    "GHCIDE_REV",
    "GHCIDE_SHA256",
)

def _impl(module_ctx):
    http_archive(
        name = "ghcide_lib",
        build_file = ":files/haskell_ghcide_lib.BUILD.bzl",
        patch_args = ["-p1"],
        patches = [
            "//bazel/patches:haskell/ghcide_binary_q.patch",
        ],
        sha256 = GHCIDE_SHA256,
        strip_prefix = "daml-ghcide-%s" % GHCIDE_REV,
        url = "{}/archive/{}.tar.gz".format(GHCIDE_REPO_URL, GHCIDE_REV),
    )

ghcide_lib = module_extension(implementation = _impl)
