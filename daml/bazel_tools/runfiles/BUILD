load("//bazel_tools:scala.bzl", "da_scala_library")

da_scala_library(
    name = "scala_runfiles",
    srcs = glob(["src/main/scala/**/*.scala"]),
    visibility = ["//visibility:public"],
    deps = [
        "@bazel_tools//tools/java/runfiles",
    ],
)
