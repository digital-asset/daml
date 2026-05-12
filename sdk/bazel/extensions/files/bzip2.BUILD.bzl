genrule(
    name = "bzip2_build",
    srcs = glob(["**"]),
    outs = [
        "lib/libbz2.so",
        "lib/libbz2.so.1",
        "include/bzlib.h",
    ],
    cmd = """
        CC=$$PWD/$(CC)
        PATCHELF=$$PWD/$(execpath @patchelf//:patchelf)
        SRC=$$(realpath $$(dirname $(location Makefile-libbz2_so)))
        PREFIX=$$(realpath $(@D))
        BUILD=$$(mktemp -d /tmp/bzip2-XXXXXX)
        cp -rpL $$SRC/. $$BUILD
        chmod -R u+w $$BUILD
        cd $$BUILD && make -f Makefile-libbz2_so -j$$(nproc) CC="$$CC" \
        && mkdir -p $$PREFIX/lib \
        && mkdir -p $$PREFIX/include \
        && cp libbz2.so.1.0.8 $$PREFIX/lib/libbz2.so.1 \
        && cp libbz2.so.1.0.8 $$PREFIX/lib/libbz2.so \
        && cp $$SRC/bzlib.h $$PREFIX/include/bzlib.h \
        && $$PATCHELF --set-soname libbz2.so $$PREFIX/lib/libbz2.so \
        && rm -rf $$BUILD
    """,
    tools = ["@patchelf//:patchelf"],
    toolchains = ["@rules_cc//cc:current_cc_toolchain"],
)

filegroup(
    name = "libs",
    srcs = [":bzip2_build"],
    visibility = ["//visibility:public"],
)

# Wraps the hermetic libbz2.so + bzlib.h as a cc_library so it can be
# passed to rules_haskell's stack_snapshot `extra_deps` (which expects
# CcInfo providers). The `bzlib-conduit` Haskell package fails Cabal's
# configure step ("Missing (or bad) C library: bz2", sdk/dump.txt:600-601)
# without this wiring -- the hermetic Bootlin sysroot ships no libbz2.
# Mirrors the `@zlib_shared//:zlib_cc_lib` pattern in zlib.BUILD.bzl.
cc_library(
    name = "bz2_cc_lib",
    srcs = ["lib/libbz2.so"],
    hdrs = ["include/bzlib.h"],
    includes = ["include"],
    visibility = ["//visibility:public"],
)
