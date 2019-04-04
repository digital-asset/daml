# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""A tiny example binary for the native Python rules of Bazel."""

import unittest
from pipeline.samples.bazel.python.lib import GetNumber
from fib import Fib


class TestGetNumber(unittest.TestCase):

  def test_ok(self):
    self.assertEquals(GetNumber(), 42)

  def test_fib(self):
    self.assertEquals(Fib(5), 8)

if __name__ == '__main__':
  unittest.main()
