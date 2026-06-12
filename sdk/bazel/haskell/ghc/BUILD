load(":ghc_bindist_install.bzl", "ghc_bindist_install")

exports_files(["ghc_packages.lock.json"])

ghc_bindist_install(
    name = "ghc_install",
    srcs = "@ghc_bindist_raw//:bindist_srcs",
    configure = "@ghc_bindist_raw//:bindist_unpacked/configure",
    make = "@make//:make",
    visibility = ["//visibility:public"],
)
