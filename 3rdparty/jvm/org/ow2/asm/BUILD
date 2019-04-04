# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "asm",
    exports = [
        "//external:jar/org/ow2/asm/asm"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "asm_analysis",
    exports = [
        "//external:jar/org/ow2/asm/asm_analysis",
        ":asm_tree"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "asm_commons",
    exports = [
        "//external:jar/org/ow2/asm/asm_commons",
        ":asm_tree"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "asm_tree",
    exports = [
        "//external:jar/org/ow2/asm/asm_tree",
        ":asm"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "asm_util",
    exports = [
        "//external:jar/org/ow2/asm/asm_util",
        ":asm_tree"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


