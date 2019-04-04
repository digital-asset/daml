# JMH Benchmark Bazel Sample

This directory contains a simple example showing how to use `rules_scala`'s
support for JMH benchmarks. The example code has been slightly adapted from:

https://github.com/bazelbuild/rules_scala/tree/4be50865a332aef46c46c94b345c320c3353e9e1/test/jmh

Build this example with the following command:
> bazel build //pipeline/samples/bazel/scala/jmh:test_benchmark

To run the benchmark, use:
> bazel run //pipeline/samples/bazel/scala/jmh:test_benchmark
