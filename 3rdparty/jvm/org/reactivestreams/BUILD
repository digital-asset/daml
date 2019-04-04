# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "reactive_streams",
    exports = [
        "//external:jar/org/reactivestreams/reactive_streams"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "reactive_streams_examples",
    exports = [
        "//external:jar/org/reactivestreams/reactive_streams_examples",
        ":reactive_streams"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "reactive_streams_tck",
    exports = [
        "//3rdparty/jvm/org/testng:testng",
        "//external:jar/org/reactivestreams/reactive_streams_tck",
        ":reactive_streams",
        ":reactive_streams_examples"
    ],
    visibility = [
        "//visibility:public"
    ]
)


