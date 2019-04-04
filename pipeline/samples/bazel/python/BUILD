filegroup(
    name = "srcs",
    srcs = glob(["*.py"]) + [
        "BUILD",
        "//pipeline/samples/bazel/python/fibonacci:srcs",
    ],
    visibility = ["//pipeline/samples/bazel:__pkg__"],
)

py_binary(
    name = "bin",
    srcs = ["bin.py"],
    deps = [
        ":lib",
        "//pipeline/samples/bazel/python/fibonacci",
    ],
)

py_library(
    name = "lib",
    srcs = ["lib.py"],
)

py_test(
    name = "test",
    srcs = ["test.py"],
    deps = [
        ":lib",
        "//pipeline/samples/bazel/python/fibonacci",
    ],
)
