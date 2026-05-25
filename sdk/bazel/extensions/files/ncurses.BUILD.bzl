load("@rules_cc//cc:defs.bzl", "cc_library")

# All hermetic ncurses .so files are real files on disk after the
# repository rule's `make install` + filename-versioning steps (see
# sdk/bazel/extensions/ncurses_extension.bzl).  exports_files lets
# other repository rules read them via ctx.path() at fetch time --
# this is what enables the GHC bindist patch
# (sdk/bazel/patches/haskell/rules_haskell_hermetic_cc.patch) to
# point LD_LIBRARY_PATH at lib/libtinfo.so.5 without depending on a
# host libtinfo5 package.
exports_files(
    glob([
        "lib/*.so",
        "lib/*.so.*",
    ]),
    visibility = ["//visibility:public"],
)

# Daml hermetic-GCC migration: haskeline's stack_snapshot `extra_deps`
# needs a cc_library whose embedded SONAME is `libtinfo.so` (not
# `libtinfow.so.6` as produced by autoconf) so that haskeline's
# emitted `-ltinfo` DT_NEEDED entries resolve at runtime via the
# hermetic ncurses build instead of a host libtinfo.  patchelf from
# BCR is a cc_binary (action-time-only), so the SONAME rewrite
# happens in this small genrule here rather than in the fetch-time
# repository rule.
genrule(
    name = "libtinfo_so",
    srcs = ["lib/libtinfow.so"],
    outs = ["lib/libtinfo.so"],
    cmd = "cp $< $@ && $(execpath @patchelf//:patchelf) --set-soname libtinfo.so $@",
    tools = ["@patchelf//:patchelf"],
)

cc_library(
    name = "tinfo_cc_lib",
    srcs = [":libtinfo_so"],
    visibility = ["//visibility:public"],
)

# Aggregates the SONAME-rewritten libtinfo.so alongside every real
# .so file produced at fetch time.  Used as data for damlc binaries.
filegroup(
    name = "libs",
    srcs = glob(["lib/*.so", "lib/*.so.*"]) + [":libtinfo_so"],
    visibility = ["//visibility:public"],
)
