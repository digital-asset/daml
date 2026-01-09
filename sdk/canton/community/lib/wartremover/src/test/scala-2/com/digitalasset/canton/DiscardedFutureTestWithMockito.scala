// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import com.digitalasset.canton.DiscardedFutureTest.{TraitWithFuture, assertErrors}
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

trait DiscardedFutureTestWithMockito extends AnyWordSpec with MockitoSugar {

  "DiscardedFuture" should {
    "allow Mockito verify calls" in {
      val result = WartTestTraverser(DiscardedFuture) {
        val mocked = mock[TraitWithFuture]
        verify(mocked).returnsFuture
        verify(mocked).returnsFutureNoArgs()
        verify(mocked).returnsFutureOneArg(0)
        verify(mocked).returnsFutureTwoArgs(1)("")
        verify(mocked).returnsFutureThreeArgs(2)("string")(new Object)
        verify(mocked).returnsFutureTypeArgs("string")
        ()
      }
      assertErrors(result, 0)
    }
  }
}
