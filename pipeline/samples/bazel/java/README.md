# Java Bazel example

##### Build:

    bazel build //pipeline/samples/bazel/java:hello-world
    bazel build //pipeline/samples/bazel/java:hello-resources
    
##### Run:

    bazel run //pipeline/samples/bazel/java:hello-world
    bazel run //pipeline/samples/bazel/java:hello-resources
    
##### Test:

###### All:

    bazel test //pipeline/samples/bazel/java/...

###### Specific:

    bazel test //pipeline/samples/bazel/java:hello
    bazel test //pipeline/samples/bazel/java:custom
