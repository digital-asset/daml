genrule(
    name = "bzip2_build",
    srcs = glob(["**"]),
    outs = [
        "lib/libbz2.so",
        "lib/libbz2.so.1",
    ],
    cmd = """
        SRC=$$(realpath $$(dirname $(location Makefile-libbz2_so)))
        PREFIX=$$(realpath $(@D))
        BUILD=$$(mktemp -d /tmp/bzip2-XXXXXX)
        cp -rpL $$SRC/. $$BUILD
        chmod -R u+w $$BUILD
        cd $$BUILD && make -f Makefile-libbz2_so -j$$(nproc) CC="$(CC)" \
        && mkdir -p $$PREFIX/lib \
        && cp libbz2.so.1.0.8 $$PREFIX/lib/libbz2.so.1 \
        && cp libbz2.so.1.0.8 $$PREFIX/lib/libbz2.so \
        && rm -rf $$BUILD
    """,
    toolchains = ["@rules_cc//cc:current_cc_toolchain"],
)

filegroup(
    name = "libs",
    srcs = [":bzip2_build"],
    visibility = ["//visibility:public"],
)
