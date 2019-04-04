# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "junit_jupiter_api",
    exports = [
        "//3rdparty/jvm/org/apiguardian:apiguardian_api",
        "//3rdparty/jvm/org/junit/platform:junit_platform_commons",
        "//3rdparty/jvm/org/opentest4j:opentest4j",
        "//external:jar/org/junit/jupiter/junit_jupiter_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "junit_jupiter_engine",
    exports = [
        "//3rdparty/jvm/org/apiguardian:apiguardian_api",
        "//3rdparty/jvm/org/junit/platform:junit_platform_engine",
        "//external:jar/org/junit/jupiter/junit_jupiter_engine",
        ":junit_jupiter_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)


