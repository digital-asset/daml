# Scala Bazel example

##### Build:

###### All:

    bazel build //pipeline/samples/bazel/scala/...

###### Specific:

    bazel build //pipeline/samples/bazel/scala:hello-world
    bazel build //pipeline/samples/bazel/scala:hello-resources
    
##### Run:

    bazel run //pipeline/samples/bazel/scala:hello-world
    bazel run //pipeline/samples/bazel/scala:hello-resources
    
##### Test:

###### All:

    bazel test //pipeline/samples/bazel/scala/...

###### Specific:

    bazel test //pipeline/samples/bazel/scala:hello
    bazel test //pipeline/samples/bazel/scala:custom
