package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_binary", "scala_library", "scala_test")

filegroup(
    name = "greeting",
    srcs = ["src/main/resources/greeting.txt"],
)

scala_binary(
    name = "hello-world",
    main_class = "com.example.myproject.Greeter",
    runtime_deps = [":hello-lib"],
)

scala_library(
    name = "hello-lib",
    srcs = glob(["src/main/scala/com/example/myproject/*.scala"]),
)

scala_binary(
    name = "hello-resources",
    main_class = "com.example.myproject.Greeter",
    runtime_deps = [":custom-greeting"],
)

scala_library(
    name = "custom-greeting",
    srcs = glob(["src/main/scala/com/example/myproject/*.scala"]),
    resources = ["//pipeline/samples/bazel/scala:greeting"],
)

scala_test(
    name = "hello",
    srcs = ["src/test/scala/com/example/myproject/TestHello.scala"],
    deps = [
        "//3rdparty/jvm/org/scalatest",
        "//pipeline/samples/bazel/scala:hello-lib",
    ],
)

scala_test(
    name = "custom",
    srcs = ["src/test/scala/com/example/myproject/TestCustomGreeting.scala"],
    deps = [
        "//3rdparty/jvm/org/scalatest",
        "//pipeline/samples/bazel/scala:custom-greeting",
    ],
)
