package(default_visibility = ["//visibility:public"])

cc_library(
    name = "libz",
    # Import `:z` as `srcs` to enforce the library name `libz.so`.
    # Othwerwise, Bazel would mangle the library name and e.g. Cabal
    # wouldn't recognize it.
    srcs = [":z"],
    hdrs = [":headers"],
    includes = ["."],
    linkstatic = 1,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "z",
    srcs = [
        "adler32.c",
        "compress.c",
        "crc32.c",
        "deflate.c",
        "infback.c",
        "inffast.c",
        "inflate.c",
        "inftrees.c",
        "trees.c",
        "uncompr.c",
        "zutil.c",
    ],
    hdrs = [":headers"],
    includes = ["."],
)

filegroup(
    name = "headers",
    srcs = [
        "crc32.h",
        "deflate.h",
        "gzguts.h",
        "inffast.h",
        "inffixed.h",
        "inflate.h",
        "inftrees.h",
        "trees.h",
        "zconf.h",
        "zlib.h",
        "zutil.h",
    ],
)
