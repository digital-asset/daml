# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Taken from https://github.com/bazelbuild/rules_scala/blob/2676400bed17b03fdd0fe13a8794eb0ff0129284/scala/scala.bzl#L413
def _sanitize_string_for_usage(s):
    res_array = []
    for idx in range(len(s)):
        c = s[idx]
        if c.isalnum() or c == ".":
            res_array.append(c)
        else:
            res_array.append("_")
    return "".join(res_array)

def _junit_class_name(test_file, strip):
    (_, _, stripped) = test_file.partition(strip)
    (sansext, _, _) = stripped.rpartition(".")
    c = sansext.replace("/", ".")
    return c

# Similar to https://github.com/bazelbuild/rules_scala/blob/2676400bed17b03fdd0fe13a8794eb0ff0129284/scala/scala.bzl#L425
def java_test_suite(name, srcs = [], strip = "", visibility = None, use_short_names = False, **kwargs):
    """Define a Java test-suite.

    Generates a java_test for each given source file and bundles them in a
    test_suite target under the given name.

    Args:
      name: Name of the final test-suite.
      srcs: Test-case source files. Generate one test-case per file.
      strip: File-name to class name prefix.

        Strip this string from the beginning of each file-name before
        generating the full class name from the file path.

      visibility: The visibility of this rule and the generated test-suites.
      kwargs: Remaining arguments are forwarded to each java_test rule.
    """
    ts = []
    i = 0
    for test_file in srcs:
        i = i + 1
        n = ("%s_%s" % (name, i)) if use_short_names else "%s_test_suite_%s" % (name, _sanitize_string_for_usage(test_file))
        native.java_test(
            name = n,
            test_class = _junit_class_name(test_file, strip),
            srcs = [test_file],
            visibility = visibility,
            **kwargs
        )
        ts.append(n)
    native.test_suite(name = name, tests = ts, visibility = visibility)
