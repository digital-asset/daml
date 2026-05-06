genrule(
    name = "zlib_build",
    srcs = glob(["**"]),
    outs = [
        "lib/libz.so",
        "lib/libz.so.1",
    ],
    cmd = """
        SRC=$$(realpath $$(dirname $(location configure)))
        PREFIX=$$(realpath $(@D))
        CC_ABS=$$PWD/$(CC)
        BUILD=$$(mktemp -d /tmp/zlib-XXXXXX)
        cp -rpL $$SRC/. $$BUILD
        chmod -R u+w $$BUILD
        cd $$BUILD && CC="$$CC_ABS" \
        ./configure \
            --prefix=$$PREFIX \
            --shared \
        && make -j$$(nproc) CC="$$CC_ABS" \
        && make install \
        && cd $$PREFIX/lib \
        && for f in *.so *.so.*; do \
            if [ -L "$$f" ]; then \
                target=$$(readlink -f "$$f"); \
                rm "$$f"; \
                cp "$$target" "$$f"; \
            fi; \
        done \
        && rm -rf $$BUILD
    """,
    toolchains = ["@rules_cc//cc:current_cc_toolchain"],
)

filegroup(
    name = "libs",
    srcs = [":zlib_build"],
    visibility = ["//visibility:public"],
)
