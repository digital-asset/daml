# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "zipkin_reporter",
    exports = [
        "//3rdparty/jvm/io/zipkin/java:zipkin",
        "//external:jar/io/zipkin/reporter/zipkin_reporter"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "zipkin_sender_okhttp3",
    exports = [
        "//3rdparty/jvm/com/squareup/okhttp3:okhttp",
        "//3rdparty/jvm/io/zipkin/java:zipkin",
        "//external:jar/io/zipkin/reporter/zipkin_sender_okhttp3",
        ":zipkin_reporter"
    ],
    visibility = [
        "//visibility:public"
    ]
)


