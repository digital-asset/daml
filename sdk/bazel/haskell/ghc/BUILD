load("@rules_python//python:defs.bzl", "py_binary", "py_test")
load(":ghc_bindist_install.bzl", "ghc_bindist_install")

exports_files(["ghc_packages.lock.json"])

ghc_bindist_install(
    name = "ghc_install",
    srcs = "@ghc_bindist_raw//:bindist_srcs",
    configure = "@ghc_bindist_raw//:bindist_unpacked/configure",
    make = "@make//:make",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "ghc_install_tree",
    srcs = [":ghc_install"],
    output_group = "install_tree",
)

py_binary(
    name = "ghc_packages.pin",
    srcs = ["pkgdb_to_lock.py"],
    main = "pkgdb_to_lock.py",
    args = [
        "$(rlocationpath :ghc_install_tree)",
        "$(rlocationpath @ghc_bindist_raw//:bindist_unpacked/configure)",
    ],
    data = [
        ":ghc_install_tree",
        "@ghc_bindist_raw//:bindist_srcs",
        "@ghc_bindist_raw//:bindist_unpacked/configure",
    ],
    deps = ["@rules_python//python/runfiles"],
)

py_test(
    name = "ghc_packages.check",
    srcs = ["pkgdb_to_lock.py"],
    main = "pkgdb_to_lock.py",
    args = [
        "--check",
        "$(rlocationpath ghc_packages.lock.json)",
        "$(rlocationpath :ghc_install_tree)",
        "$(rlocationpath @ghc_bindist_raw//:bindist_unpacked/configure)",
    ],
    data = [
        "ghc_packages.lock.json",
        ":ghc_install_tree",
        "@ghc_bindist_raw//:bindist_srcs",
        "@ghc_bindist_raw//:bindist_unpacked/configure",
    ],
    deps = ["@rules_python//python/runfiles"],
)
