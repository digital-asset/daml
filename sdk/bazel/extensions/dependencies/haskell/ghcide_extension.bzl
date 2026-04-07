load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    GHCIDE_REV = "2914f9328dc549bd4918c3cf0fc008690a102d70"
    GHCIDE_SHA256 = "bc2bd2fbfbfb501e303b35fb4b2f503b122e4eb0aff34472f0e882d2289afc57"

    http_archive(
        name = "ghcide_ghc_lib",
        build_file = ":files/haskell_ghcide_ghc_lib.BUILD.bzl",
        patch_args = ["-p1"],
        patches = [
            "//bazel/patches:haskell/ghcide_binary_q.patch",
        ],
        sha256 = GHCIDE_SHA256,
        strip_prefix = "daml-ghcide-%s" % GHCIDE_REV,
        url = "https://github.com/digital-asset/daml-ghcide/archive/%s.tar.gz" % GHCIDE_REV,
    )

ghcide = module_extension(implementation = _impl)
