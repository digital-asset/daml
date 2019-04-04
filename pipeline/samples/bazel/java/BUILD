package(default_visibility = ["//visibility:public"])

filegroup(
    name = "greeting",
    srcs = ["src/main/resources/greeting.txt"],
)

java_binary(
    name = "hello-world",
    main_class = "com.example.myproject.Greeter",
    runtime_deps = [":hello-lib"],
)

java_library(
    name = "hello-lib",
    srcs = ["src/main/java/com/example/myproject/Greeter.java"],
)

java_binary(
    name = "hello-resources",
    main_class = "com.example.myproject.Greeter",
    runtime_deps = [":custom-greeting"],
)

java_library(
    name = "custom-greeting",
    srcs = ["src/main/java/com/example/myproject/Greeter.java"],
    resources = ["//pipeline/samples/bazel/java:greeting"],
)

java_test(
    name = "hello",
    srcs = ["src/test/java/com/example/myproject/TestHello.java"],
    test_class = "com.example.myproject.TestHello",
    deps = [
        "//pipeline/samples/bazel/java:hello-lib",
        "//3rdparty/jvm/junit:junit",
    ],
)

java_test(
    name = "custom",
    srcs = ["src/test/java/com/example/myproject/TestCustomGreeting.java"],
    test_class = "com.example.myproject.TestCustomGreeting",
    deps = [
        "//pipeline/samples/bazel/java:custom-greeting",
        "//3rdparty/jvm/junit:junit",
    ],
)