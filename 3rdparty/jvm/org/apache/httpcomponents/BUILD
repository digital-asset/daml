# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "httpclient",
    exports = [
        "//3rdparty/jvm/commons_codec:commons_codec",
        "//3rdparty/jvm/commons_logging:commons_logging",
        "//external:jar/org/apache/httpcomponents/httpclient",
        ":httpcore"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "httpcore",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpcore"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


