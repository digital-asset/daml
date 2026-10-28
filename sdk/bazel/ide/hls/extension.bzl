load("//bazel/versions:ghc.version.bzl", "GHC_VERSION")
load("//bazel/versions:hls.version.bzl", "HLS_BINDISTS", "HLS_URL")

_BUILD_FILE = """\
filegroup(
    name = "binary",
    srcs = ["{root}/bin/{binary}"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "libraries",
    srcs = glob(["{root}/lib/{ghc_version}/**"]),
    visibility = ["//visibility:public"],
)
"""

_BINARY_PREFIX = "haskell-language-server-"

def _only_entry(rctx, directory):
    entries = rctx.path(directory).readdir()
    if len(entries) != 1:
        fail("expected a single top-level directory in {}, found {}".format(
            rctx.attr.url,
            [entry.basename for entry in entries],
        ))
    return entries[0].basename

def _hls_bindist_impl(rctx):
    rctx.download_and_extract(
        url = rctx.attr.url,
        sha256 = rctx.attr.sha256,
        output = "dist",
    )
    root = "dist/" + _only_entry(rctx, "dist")
    binary = _BINARY_PREFIX + rctx.attr.ghc_version
    if not rctx.path("{}/bin/{}".format(root, binary)).exists:
        fail("{} does not provide {}".format(rctx.attr.url, binary))
    for entry in rctx.path(root + "/bin").readdir():
        if entry.basename == binary:
            continue
        other_ghc_version = entry.basename[len(_BINARY_PREFIX):] if entry.basename.startswith(_BINARY_PREFIX) else ""
        if other_ghc_version[:1].isdigit():
            rctx.delete("{}/lib/{}".format(root, other_ghc_version))
        rctx.delete(entry)
    rctx.file("BUILD.bazel", _BUILD_FILE.format(
        root = root,
        binary = binary,
        ghc_version = rctx.attr.ghc_version,
    ))

hls_bindist = repository_rule(
    implementation = _hls_bindist_impl,
    attrs = {
        "ghc_version": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
        "url": attr.string(mandatory = True),
    },
)

def _hls_impl(module_ctx):
    for (os, cpu), bindist in HLS_BINDISTS.items():
        hls_bindist(
            name = "hls_{}_{}".format(os, cpu),
            ghc_version = GHC_VERSION,
            sha256 = bindist["sha256"],
            url = HLS_URL.format(version = bindist["version"], triple = bindist["triple"]),
        )

hls = module_extension(implementation = _hls_impl)
