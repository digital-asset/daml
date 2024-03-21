// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class VoidValueRefSpec extends AnyWordSpec with Matchers {
  "VoidValueRef subclasses" should {
    sealed abstract class TestVoid extends VoidValueRef
    // NB: *no special companion is required!*
    object TestVoid
    identity(TestVoid)

    "always have a codec instance" in {
      Value.Encoder[TestVoid]
      Value.Decoder[TestVoid]
    }

    /* XXX needs gen for Value
    "never succeed decoding" in forAll(argumentValueGen(1)) { av =>
      Value.decode[TestVoid](av) shouldBe None
    }
     */
  }
}
