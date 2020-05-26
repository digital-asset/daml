package(default_visibility = ["//visibility:public"])

cc_library(
    name = "libbz2",
    # Import `:bz2` as `srcs` to enforce the library name `libbz2.so`.
    # Othwerwise, Bazel would mangle the library name and e.g. Cabal
    # wouldn't recognize it.
    srcs = [":bz2"],
    hdrs = [":headers"],
    includes = ["."],
    linkstatic = 1,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "bz2",
    srcs = [
        "blocksort.c",
        "bzlib.c",
        "compress.c",
        "crctable.c",
        "decompress.c",
        "huffman.c",
        "randtable.c",
    ],
    hdrs = [":headers"],
    includes = ["."],
)

filegroup(
    name = "headers",
    srcs = [
        "bzlib.h",
        "bzlib_private.h",
    ],
)
