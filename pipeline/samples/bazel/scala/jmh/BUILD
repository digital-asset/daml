load(
    "@io_bazel_rules_scala//jmh:jmh.bzl",
    "scala_benchmark_jmh",
)

load(
    "//bazel_tools:scala.bzl",
    "da_scala_library"
)

java_library(
    name = "java_type",
    srcs = ["JavaType.java"],
    visibility = ["//visibility:public"],
)

da_scala_library(
    name = "scala_type",
    srcs = ["ScalaType.scala"],
    visibility = ["//visibility:public"],
)

da_scala_library(
    name = "add_numbers",
    srcs = ["AddNumbers.scala"],
    visibility = ["//visibility:public"],
    exports = [
        ":java_type",
        ":scala_type",
    ],
    deps = [
        ":java_type",
        ":scala_type",
    ],
)

scala_benchmark_jmh(
    name = "test_benchmark",
    srcs = ["TestBenchmark.scala"],
    deps = [":add_numbers"],
)
