// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import org.scalactic.source.Position
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpecLike

/** This trait allows the use of `inUS` in place of `in` when working with tests that return
  * FutureUnlessShutdown
  */
trait InUS {

  this: AsyncWordSpecLike =>

  implicit class InUsStringWrapper(s: String) {
    def inUS(f: => FutureUnlessShutdown[Assertion])(implicit pos: Position): Unit =
      s in f.onShutdown(fail(s"Unexpected shutdown in StringWrapperUS.inUS"))
  }

}
