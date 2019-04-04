# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=superfluous-parens
"""A tiny example binary for the native Python rules of Bazel."""
from pipeline.samples.bazel.python.lib import GetNumber
from fib import Fib

print("The number is %d" % GetNumber())
print("Fib(5) == %d" % Fib(5))
