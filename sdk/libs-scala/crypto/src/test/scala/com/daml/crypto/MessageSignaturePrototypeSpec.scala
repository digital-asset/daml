// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MessageSignaturePrototypeSpec extends AnyFlatSpec with Matchers {
  behavior of MessageSignaturePrototype.getClass.getSimpleName

  it should "expose algorithm" in {
    MessageSignaturePrototype.Secp256k1.algorithm shouldBe "SHA256withECDSA"
  }
}
