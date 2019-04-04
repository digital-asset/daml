# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "mockito_core",
    exports = [
        "//3rdparty/jvm/net/bytebuddy:byte_buddy",
        "//3rdparty/jvm/net/bytebuddy:byte_buddy_agent",
        "//3rdparty/jvm/org/objenesis:objenesis",
        "//external:jar/org/mockito/mockito_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "mockito_inline",
    exports = [
        "//external:jar/org/mockito/mockito_inline",
        ":mockito_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)


