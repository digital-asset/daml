load("@ghc_bindist_raw//:libraries.bzl", "TOOLCHAIN_LIBRARIES")
load("@rules_haskell//haskell:defs.bzl", "haskell_toolchain")
load("@rules_python//python:defs.bzl", "py_binary", "py_test")
load(":ghc_bindist_install.bzl", "ghc_bindist_install")

exports_files(["ghc_packages.lock.json"])

ghc_bindist_install(
    name = "ghc_install",
    srcs = "@ghc_bindist_raw//:bindist_srcs",
    configure = "@ghc_bindist_raw//:bindist_unpacked/configure",
    gmp = "@gmp//:libs",
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

haskell_toolchain(
    name = "ghc",
    libdir = [":ghc_install"],
    libraries = TOOLCHAIN_LIBRARIES,
    docdir_path = "doc",
    static_runtime = False,
    fully_static_link = False,
    tools = [":ghc_install"],
    version = "9.0.2",
)

toolchain(
    name = "ghc_linux_amd64",
    exec_compatible_with = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    target_compatible_with = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    toolchain = ":ghc",
    toolchain_type = "@rules_haskell//haskell:toolchain",
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
