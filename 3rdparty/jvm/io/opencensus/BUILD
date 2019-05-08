# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "opencensus_api",
    exports = [
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//external:jar/io/opencensus/opencensus_api"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "opencensus_contrib_grpc_metrics",
    exports = [
        "//external:jar/io/opencensus/opencensus_contrib_grpc_metrics",
        ":opencensus_api"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "opencensus_contrib_http_util",
    exports = [
        "//3rdparty/jvm/com/google/guava:guava",
        "//external:jar/io/opencensus/opencensus_contrib_http_util",
        ":opencensus_api"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


