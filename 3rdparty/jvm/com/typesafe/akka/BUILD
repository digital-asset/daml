# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
scala_import(
    name = "akka_actor",
    exports = [
        "//3rdparty/jvm/com/typesafe:config",
        "//3rdparty/jvm/org/scala_lang/modules:scala_java8_compat",
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_actor_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "akka_http",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":akka_http_core",
        ":akka_stream"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_http_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "akka_http_core",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":akka_parsing",
        ":akka_stream"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_http_core_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "akka_http_spray_json",
    exports = [
        "//3rdparty/jvm/io/spray:spray_json",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":akka_http",
        ":akka_stream"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_http_spray_json_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "akka_http_testkit",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":akka_http",
        ":akka_stream_testkit"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_http_testkit_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "akka_parsing",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":akka_actor"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_parsing_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "akka_protobuf",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_protobuf_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "akka_slf4j",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        ":akka_actor"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_slf4j_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "akka_stream",
    exports = [
        "//3rdparty/jvm/com/typesafe:ssl_config_core",
        "//3rdparty/jvm/org/reactivestreams:reactive_streams",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":akka_actor",
        ":akka_protobuf"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_stream_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "akka_stream_testkit",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":akka_stream",
        ":akka_testkit"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_stream_testkit_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "akka_testkit",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":akka_actor"
    ],
    jars = [
        "//external:jar/com/typesafe/akka/akka_testkit_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


