# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "netty_buffer",
    exports = [
        "//external:jar/io/netty/netty_buffer",
        ":netty_common"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "netty_codec",
    exports = [
        "//external:jar/io/netty/netty_codec",
        ":netty_buffer",
        ":netty_common",
        ":netty_transport"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "netty_codec_http",
    exports = [
        "//external:jar/io/netty/netty_codec_http",
        ":netty_buffer",
        ":netty_codec",
        ":netty_common",
        ":netty_transport"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "netty_codec_http2",
    exports = [
        "//external:jar/io/netty/netty_codec_http2",
        ":netty_buffer",
        ":netty_codec",
        ":netty_codec_http",
        ":netty_common",
        ":netty_handler",
        ":netty_transport"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_codec_socks",
    exports = [
        "//external:jar/io/netty/netty_codec_socks",
        ":netty_buffer",
        ":netty_codec",
        ":netty_common",
        ":netty_transport"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "netty_common",
    exports = [
        "//external:jar/io/netty/netty_common"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "netty_handler",
    exports = [
        "//external:jar/io/netty/netty_handler",
        ":netty_buffer",
        ":netty_codec",
        ":netty_common",
        ":netty_transport"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_handler_proxy",
    exports = [
        "//external:jar/io/netty/netty_handler_proxy",
        ":netty_buffer",
        ":netty_codec",
        ":netty_codec_http",
        ":netty_codec_socks",
        ":netty_common",
        ":netty_transport"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_resolver",
    exports = [
        "//external:jar/io/netty/netty_resolver",
        ":netty_common"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_tcnative_boringssl_static",
    exports = [
        "//external:jar/io/netty/netty_tcnative_boringssl_static"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_transport",
    exports = [
        "//external:jar/io/netty/netty_transport",
        ":netty_buffer",
        ":netty_common",
        ":netty_resolver"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


