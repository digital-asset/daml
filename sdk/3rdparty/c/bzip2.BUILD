cc_binary(
    name = "bzip2",
    srcs = ["bzip2.c"],
    defines = [
        "_FILE_OFFSET_BITS=64",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":bz2",
    ],
)

cc_library(
    name = "libbz2",
    # Import `:bz2` as `srcs` to enforce the library name `libbz2.so`.
    # Othwerwise, Bazel would mangle the library name and e.g. Cabal
    # wouldn't recognize it.
    srcs = [":bz2"],
    hdrs = ["bzlib.h"],
    includes = ["."],
    linkstatic = 1,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "bz2",
    srcs = [
        "blocksort.c",
        "bzlib.c",
        "bzlib_private.h",
        "compress.c",
        "crctable.c",
        "decompress.c",
        "huffman.c",
        "randtable.c",
    ],
    hdrs = [
        "bzlib.h",
    ],
    defines = [
        "_FILE_OFFSET_BITS=64",
    ],
    includes = ["."],
    strip_include_prefix = "",
    visibility = ["//visibility:public"],
)
