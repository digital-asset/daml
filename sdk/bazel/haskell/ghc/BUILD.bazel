load("@ghc_bindist//:libraries.bzl", "TOOLCHAIN_LIBRARIES")
load("@rules_haskell//haskell:defs.bzl", "haskell_toolchain")
load("@rules_python//python:defs.bzl", "py_binary", "py_test")

exports_files(["ghc_packages.lock.json"])

filegroup(
    name = "ghc_install_tree",
    srcs = ["//bazel/haskell/toolchain:ghc_install"],
    output_group = "install_tree",
)

py_binary(
    name = "ghc_packages.pin",
    srcs = ["pkgdb_to_lock.py"],
    main = "pkgdb_to_lock.py",
    args = [
        "$(rlocationpath :ghc_install_tree)",
        "$(rlocationpath @ghc_bindist//:bindist_unpacked/configure)",
    ],
    data = [
        ":ghc_install_tree",
        "@ghc_bindist//:bindist_srcs",
        "@ghc_bindist//:bindist_unpacked/configure",
    ],
    deps = ["@rules_python//python/runfiles"],
)

haskell_toolchain(
    name = "ghc",
    libdir = ["//bazel/haskell/toolchain:ghc_install"],
    libraries = TOOLCHAIN_LIBRARIES,
    docdir_path = "doc",
    static_runtime = False,
    fully_static_link = False,
    tools = ["//bazel/haskell/toolchain:ghc_install"],
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
        "$(rlocationpath @ghc_bindist//:bindist_unpacked/configure)",
    ],
    data = [
        "ghc_packages.lock.json",
        ":ghc_install_tree",
        "@ghc_bindist//:bindist_srcs",
        "@ghc_bindist//:bindist_unpacked/configure",
    ],
    deps = ["@rules_python//python/runfiles"],
)
