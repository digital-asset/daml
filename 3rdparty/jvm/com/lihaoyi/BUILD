# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
scala_import(
    name = "fansi",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":sourcecode"
    ],
    jars = [
        "//external:jar/com/lihaoyi/fansi_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "fastparse",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":fastparse_utils",
        ":sourcecode"
    ],
    jars = [
        "//external:jar/com/lihaoyi/fastparse_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "fastparse_utils",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":sourcecode"
    ],
    jars = [
        "//external:jar/com/lihaoyi/fastparse_utils_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "pprint",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":fansi",
        ":sourcecode"
    ],
    jars = [
        "//external:jar/com/lihaoyi/pprint_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "sourcecode",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/com/lihaoyi/sourcecode_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


