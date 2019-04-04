# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""An example binary to test the imports attribute of native Python rules."""


def Fib(n):
  if n == 0 or n == 1:
    return 1
  else:
    return Fib(n-1) + Fib(n-2)
