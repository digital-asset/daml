# bzip2 builds natively as a cc_library: it has no `configure` step (just a
# plain Makefile) and ships its headers in-tree, so Bazel's C++ actions
# compile it directly via the hermetic toolchain -- no genrule, no host make.
#
# Exposes `bz2_cc_lib` for rules_haskell's stack_snapshot `extra_deps` (which
# expects CcInfo). The `bzlib-conduit` Haskell package fails Cabal's configure
# step ("Missing (or bad) C library: bz2") without this; the hermetic sysroot
# ships no libbz2.
cc_library(
    name = "bz2_cc_lib",
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
    hdrs = ["bzlib.h"],
    copts = ["-D_FILE_OFFSET_BITS=64"],
    includes = ["."],
    visibility = ["//visibility:public"],
)
