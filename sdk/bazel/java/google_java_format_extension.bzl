# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@rules_java//java:http_jar.bzl", "http_jar")
load(
    "//bazel/versions:google_java_format.version.bzl",
    "GOOGLE_JAVA_FORMAT_SHA256",
    "GOOGLE_JAVA_FORMAT_VERSION",
)

def _impl(module_ctx):
    http_jar(
        name = "google_java_format",
        sha256 = GOOGLE_JAVA_FORMAT_SHA256,
        urls = ["https://github.com/google/google-java-format/releases/download/google-java-format-{0}/google-java-format-{0}-all-deps.jar".format(GOOGLE_JAVA_FORMAT_VERSION)],
    )

google_java_format = module_extension(implementation = _impl)
