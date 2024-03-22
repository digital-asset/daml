load("@daml//bazel_tools:haskell.bzl", "da_haskell_library")
load("//:versions.bzl", "latest_stable_version")

exports_files([
    "daml_ledger_test.sh",
    "create_daml_app_test.sh",
    "sandbox-with-postgres.sh",
    "daml.cc",
])

genrule(
    name = "versions-hs-lib-gen",
    srcs = [],
    outs = ["Versions.hs"],
    cmd = """
        cat > $@ <<EOF
module Versions where

latestStableVersion :: String
latestStableVersion = "{}"
EOF
""".format(latest_stable_version),
)

da_haskell_library(
    name = "versions-hs-lib",
    srcs = [":Versions.hs"],
    hackage_deps = ["base"],
    visibility = ["//visibility:public"],
)
