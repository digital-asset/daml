// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

// lives in a separate class so that we can test a situation without imports
class PrettyPrintingImplicitResolutionTest {
  def testImplicitResolution[A: Pretty](x: A): Unit = ()

  def testImplicitResultion2[A <: PrettyPrinting](x: A): Unit = testImplicitResolution(x)
}
