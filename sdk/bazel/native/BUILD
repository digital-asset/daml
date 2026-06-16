load("@rules_cc//cc:defs.bzl", "cc_library")
load(":configure_make.bzl", "configure_make")

# TODO: ship hermetic libtinfo.so.5 and point ghc/ghc-pkg LD_LIBRARY_PATH at it to silence warning
configure_make(
    name = "tinfo",
    srcs = "@ncurses//:srcs",
    configure = "@ncurses//:configure",
    make = "@make//:make",
    configure_flags = [
        "--build=x86_64-unknown-linux-gnu",
        "--host=x86_64-pc-linux-gnu",
        "--with-shared",
        "--with-termlib",
        "--enable-widec",
        "--without-debug",
        "--without-ada",
        "--without-manpages",
        "--without-tests",
    ],
    install_libdir = "lib",
    outs = ["libtinfow.so.6"],
)

# haskeline emits `-ltinfo`; the linker needs a libtinfo.so whose SONAME is
# libtinfo.so (autoconf produces libtinfow.so.6).
genrule(
    name = "tinfo_so",
    srcs = [":tinfo"],
    outs = ["libtinfo.so"],
    cmd = "cp $(location :tinfo) $@ && chmod +w $@ && $(execpath @patchelf//:patchelf) --set-soname libtinfo.so $@",
    tools = ["@patchelf//:patchelf"],
)

cc_library(
    name = "tinfo_cc_lib",
    srcs = [":tinfo_so"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "libs",
    srcs = [
        ":tinfo",
        ":tinfo_so",
    ],
    visibility = ["//visibility:public"],
)
