// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.{BaseTest, DiscardedFuture, DiscardedFutureTest}
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

class AsyncResultTest extends AnyWordSpec with BaseTest {
  "DiscardedFuture" should {
    "detect discarded AsyncResult" in {
      val result = WartTestTraverser(DiscardedFuture) {
        AsyncResult(FutureUnlessShutdown.pure(()))
        ()
      }
      DiscardedFutureTest.assertErrors(result, 1)
    }
  }
}
