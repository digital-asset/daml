load("@daml//bazel_tools:haskell.bzl", "da_haskell_binary")

da_haskell_binary(
    name = "update-versions",
    srcs = ["UpdateVersions.hs"],
    hackage_deps = [
        "aeson",
        "async",
        "base",
        "bytestring",
        "containers",
        "cryptonite",
        "extra",
        "http-client",
        "http-conduit",
        "lens",
        "memory",
        "optparse-applicative",
        "process",
        "semver",
        "text",
        "unordered-containers",
    ],
)
