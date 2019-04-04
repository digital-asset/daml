# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "junit_platform_commons",
    exports = [
        "//3rdparty/jvm/org/apiguardian:apiguardian_api",
        "//external:jar/org/junit/platform/junit_platform_commons"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "junit_platform_engine",
    exports = [
        "//3rdparty/jvm/org/apiguardian:apiguardian_api",
        "//3rdparty/jvm/org/opentest4j:opentest4j",
        "//external:jar/org/junit/platform/junit_platform_engine",
        ":junit_platform_commons"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "junit_platform_launcher",
    exports = [
        "//3rdparty/jvm/org/apiguardian:apiguardian_api",
        "//external:jar/org/junit/platform/junit_platform_launcher",
        ":junit_platform_engine"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "junit_platform_runner",
    exports = [
        "//3rdparty/jvm/junit:junit",
        "//3rdparty/jvm/org/apiguardian:apiguardian_api",
        "//external:jar/org/junit/platform/junit_platform_runner",
        ":junit_platform_launcher",
        ":junit_platform_suite_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "junit_platform_suite_api",
    exports = [
        "//3rdparty/jvm/org/apiguardian:apiguardian_api",
        "//external:jar/org/junit/platform/junit_platform_suite_api",
        ":junit_platform_commons"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


