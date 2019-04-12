load(
    "@io_tweag_rules_haskell//haskell:haskell.bzl",
    "haskell_binary",
    "haskell_import",
    "haskell_library",
)
load("@ai_formation_hazel//:hazel.bzl", "hazel_library")

haskell_import(name = "base")

haskell_binary(
    name = "hello",
    srcs = ["Hello.hs"],
    deps = [":base"],
)

haskell_library(
    name = "sop-example",
    srcs = ["SOP.hs"],
    deps = [
        ":base",
        hazel_library("generics-sop"),
    ],
)
