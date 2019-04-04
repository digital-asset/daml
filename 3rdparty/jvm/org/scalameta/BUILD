# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
scala_import(
    name = "common",
    exports = [
        "//3rdparty/jvm/com/lihaoyi:sourcecode",
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/org/scalameta/common_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "contrib",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":scalameta"
    ],
    jars = [
        "//external:jar/org/scalameta/contrib_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "dialects",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common"
    ],
    jars = [
        "//external:jar/org/scalameta/dialects_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "inline",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":inputs"
    ],
    jars = [
        "//external:jar/org/scalameta/inline_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "inputs",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common",
        ":io"
    ],
    jars = [
        "//external:jar/org/scalameta/inputs_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "io",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common"
    ],
    jars = [
        "//external:jar/org/scalameta/io_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "paradise_2_12_6",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_compiler",
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/org/scalameta/paradise_2_12_6"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "parsers",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common",
        ":dialects",
        ":inputs",
        ":tokenizers",
        ":tokens",
        ":trees"
    ],
    jars = [
        "//external:jar/org/scalameta/parsers_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "quasiquotes",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common",
        ":dialects",
        ":inputs",
        ":parsers",
        ":trees"
    ],
    jars = [
        "//external:jar/org/scalameta/quasiquotes_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "scalameta",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common",
        ":dialects",
        ":inline",
        ":inputs",
        ":io",
        ":parsers",
        ":quasiquotes",
        ":semantic",
        ":tokenizers",
        ":transversers",
        ":trees"
    ],
    jars = [
        "//external:jar/org/scalameta/scalameta_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "semantic",
    exports = [
        "//3rdparty/jvm/com/trueaccord/scalapb:scalapb_runtime_2_12",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common",
        ":parsers",
        ":trees"
    ],
    jars = [
        "//external:jar/org/scalameta/semantic_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "tokenizers",
    exports = [
        "//3rdparty/jvm/com/lihaoyi:fastparse",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common",
        ":dialects",
        ":inputs",
        ":tokens"
    ],
    jars = [
        "//external:jar/org/scalameta/tokenizers_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "tokens",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common",
        ":dialects",
        ":inputs"
    ],
    jars = [
        "//external:jar/org/scalameta/tokens_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "transversers",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common",
        ":trees"
    ],
    jars = [
        "//external:jar/org/scalameta/transversers_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "trees",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":common",
        ":dialects",
        ":inputs",
        ":tokenizers",
        ":tokens"
    ],
    jars = [
        "//external:jar/org/scalameta/trees_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


