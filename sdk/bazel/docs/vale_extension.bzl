# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//bazel/docs:vale.bzl", "vale")
load("//bazel/versions:docutils.version.bzl", "DOCUTILS_SHA256", "DOCUTILS_VERSION")
load("//bazel/versions:vale.version.bzl", "VALE_SHA256", "VALE_VERSION")

_DOCUTILS_BUILD = """\
load("@rules_python//python:defs.bzl", "py_library")

py_library(
    name = "docutils",
    srcs = glob(["docutils/**/*.py"]),
    data = glob(["docutils/**"], exclude = ["docutils/**/*.py"]),
    imports = ["."],
    visibility = ["//visibility:public"],
)
"""

def _impl(module_ctx):
    vale(
        name = "vale",
        sha256 = VALE_SHA256,
        version = VALE_VERSION,
    )
    http_archive(
        name = "docutils",
        build_file_content = _DOCUTILS_BUILD,
        sha256 = DOCUTILS_SHA256,
        type = "zip",
        urls = ["https://files.pythonhosted.org/packages/py3/d/docutils/docutils-{0}-py3-none-any.whl".format(DOCUTILS_VERSION)],
    )

vale_extension = module_extension(implementation = _impl)
