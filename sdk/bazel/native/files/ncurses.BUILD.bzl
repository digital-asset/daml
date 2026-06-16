filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

exports_files(["configure"], visibility = ["//visibility:public"])
