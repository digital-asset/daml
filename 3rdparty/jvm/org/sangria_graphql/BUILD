# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
scala_import(
    name = "macro_visit",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/scala_lang:scala_reflect"
    ],
    jars = [
        "//external:jar/org/sangria_graphql/macro_visit_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "sangria",
    exports = [
        "//3rdparty/jvm/org/parboiled:parboiled",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/scala_lang:scala_reflect",
        ":macro_visit",
        ":sangria_marshalling_api",
        ":sangria_streaming_api"
    ],
    jars = [
        "//external:jar/org/sangria_graphql/sangria_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "sangria_marshalling_api",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/org/sangria_graphql/sangria_marshalling_api_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "sangria_spray_json",
    exports = [
        "//3rdparty/jvm/io/spray:spray_json",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":sangria_marshalling_api"
    ],
    jars = [
        "//external:jar/org/sangria_graphql/sangria_spray_json_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "sangria_streaming_api",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/org/sangria_graphql/sangria_streaming_api_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


