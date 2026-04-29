genrule(
    name = "gmp_build",
    srcs = glob(["**"]),
    outs = [
        "lib/libgmp.so",
        "lib/libgmp.so.10",
    ],
    cmd = """
        M4=$$PWD/$(execpath @m4//:m4_binary)
        SRC=$$(dirname $(location configure))
        PREFIX=$$(realpath $(@D))
        cd $$SRC && CC="$(CC)" AR="$(AR)" M4=$$M4 \
        ./configure \
            --prefix=$$PREFIX \
            --with-shared \
            --disable-static \
        && make -j$$(nproc) CC="$(CC)" AR="$(AR)" \
        && make install \
        && cd $$PREFIX/lib \
        && for f in *.so *.so.*; do \
            if [ -L "$$f" ]; then \
                target=$$(readlink -f "$$f"); \
                rm "$$f"; \
                cp "$$target" "$$f"; \
            fi; \
        done
    """,
    tools = ["@m4//:m4_binary"],
    toolchains = ["@rules_cc//cc:current_cc_toolchain"],
)

filegroup(
    name = "libs",
    srcs = [":gmp_build"],
    visibility = ["//visibility:public"],
)
