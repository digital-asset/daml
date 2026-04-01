genrule(
    name = "install",
    srcs = glob(["**"]),
    outs = ["m4_install"],
    cmd = """
        set -euo pipefail
        SRC=$$PWD/$$(dirname $(location configure))
        OUT=$$PWD/$@
        mkdir -p $$OUT
        cd $$SRC
        ./configure --prefix=$$OUT
        make -j
        make install
    """,
    visibility = ["//visibility:public"],
)