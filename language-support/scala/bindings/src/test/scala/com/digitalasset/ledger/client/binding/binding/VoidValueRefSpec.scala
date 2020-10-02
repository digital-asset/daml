// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import org.scalatest.{WordSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class VoidValueRefSpec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {
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
