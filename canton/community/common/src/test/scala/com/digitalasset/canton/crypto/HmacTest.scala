// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class HmacTest extends AnyWordSpec with BaseTest {

  private lazy val longString = PseudoRandom.randomAlphaNumericString(256)
  private lazy val crypto = new SymbolicPureCrypto

  forAll(HmacAlgorithm.algorithms) { algorithm =>
    s"HMAC ${algorithm.name}" should {

      "serializing and deserializing via protobuf" in {
        val secret = HmacSecret.generate(crypto)
        val hmac =
          Hmac
            .compute(secret, ByteString.copyFromUtf8(longString), algorithm)
            .valueOr(err => fail(err.toString))
        val hmacP = hmac.toProtoV30
        Hmac.fromProtoV0(hmacP).value shouldBe (hmac)
      }

    }

  }

}
