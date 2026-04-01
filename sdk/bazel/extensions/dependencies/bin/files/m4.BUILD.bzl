genrule(
    name = "m4",
    srcs = glob(["**"]),
    outs = ["m4"],
    toolchains = ["@rules_cc//cc:current_cc_toolchain"],
    cmd = """
        set -euo pipefail

        OUT=$$PWD/$@
        SRC=$$(dirname $(location configure))

        cd $$SRC

        CC=$(CC) ./configure
        make -j

        cp src/m4 $$OUT
    """,
    visibility = ["//visibility:public"],
)