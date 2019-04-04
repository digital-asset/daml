# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
scala_import(
    name = "pureconfig",
    exports = [
        "//3rdparty/jvm/com/chuusai:shapeless",
        "//3rdparty/jvm/com/typesafe:config",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":pureconfig_macros"
    ],
    jars = [
        "//external:jar/com/github/pureconfig/pureconfig_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "pureconfig_macros",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_compiler",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/scala_lang:scala_reflect",
        "//3rdparty/jvm/org/typelevel:macro_compat"
    ],
    jars = [
        "//external:jar/com/github/pureconfig/pureconfig_macros_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


