load("@daml-sdk//bazel_tools:haskell.bzl", "c2hs_suite")

c2hs_suite(
    name = "grpc-haskell-core",
    srcs = [
        "src/Network/GRPC/Unsafe/Constants.hsc",
    ] + glob(["src/**/*.hs"]),
    c2hs_src_strip_prefix = "src",
    c2hs_srcs = [
        "src/Network/GRPC/Unsafe/Time.chs",
        "src/Network/GRPC/Unsafe/ChannelArgs.chs",
        "src/Network/GRPC/Unsafe/Slice.chs",
        "src/Network/GRPC/Unsafe/ByteBuffer.chs",
        "src/Network/GRPC/Unsafe/Metadata.chs",
        "src/Network/GRPC/Unsafe/Op.chs",
        "src/Network/GRPC/Unsafe.chs",
        "src/Network/GRPC/Unsafe/Security.chs",
    ],
    compiler_flags = ["-XCPP", "-Wno-unused-imports", "-Wno-unused-record-wildcards"],
    hackage_deps = ["clock", "managed", "base", "sorted-list", "bytestring", "containers", "stm", "transformers", "template-haskell"],
    visibility = ["//visibility:public"],
    deps = [
        "@grpc_haskell_core_cbits//:merged_cbits",
    ],
)
