load("@bazel_skylib//lib:dicts.bzl", "dicts")
load("@rules_haskell//haskell:cabal.bzl", "stack_snapshot")

def _haskell_deps_impl(ctx):
    """Module extension for Haskell dependencies."""

    for mod in ctx.modules:
        for snapshot in mod.tags.snapshot:
            stack_snapshot(
                name = snapshot.name,
                extra_deps = {
                    "zlib": ["@zlib//:zlib"],
                },
                stack_snapshot_json = snapshot.snapshot_json or "//:stackage_snapshot.json",
                haddock = False,
                local_snapshot = snapshot.local_snapshot,
                packages = [
                    "aeson",
                    "aeson-extra",
                    "async",
                    "base",
                    "bytestring",
                    "conduit",
                    "conduit-extra",
                    "containers",
                    "cryptonite",
                    "directory",
                    "extra",
                    "filepath",
                    "http-client",
                    "http-conduit",
                    "jwt",
                    "lens",
                    "lens-aeson",
                    "memory",
                    "monad-loops",
                    "mtl",
                    "network",
                    "optparse-applicative",
                    "process",
                    "safe",
                    "safe-exceptions",
                    "semver",
                    "split",
                    "stm",
                    "tagged",
                    "tar-conduit",
                    "tasty",
                    "tasty-hunit",
                    "text",
                    "time",
                    "typed-process",
                    "unix-compat",
                    "unordered-containers",
                    "utf8-string",
                    "uuid",
                ],
                components = {
                    "attoparsec": [
                        "lib:attoparsec",
                        "lib:attoparsec-internal",
                    ],
                },
                components_dependencies = {
                    "attoparsec": """{"lib:attoparsec": ["lib:attoparsec-internal"]}""",
                },
            )

_snapshot = tag_class(
    attrs = {
        "name": attr.string(mandatory = True),
        "local_snapshot": attr.label(mandatory = True),
        "snapshot_json": attr.label(),
    },
)

haskell_deps = module_extension(
    implementation = _haskell_deps_impl,
    tag_classes = {
        "snapshot": _snapshot,
    },
)
