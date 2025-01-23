// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package cctp

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.security.Security

class MessageDigestTest extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    val _ = Security.insertProviderAt(new BouncyCastleProvider, 1)
  }

  "correctly keccak-256 digest messages" in {
    val message = Ref.HexString.assertFromString("deadbeef")
    val expectedDigest = Ref.HexString.assertFromString(
      "d4fd4e189132273036449fc9e11198c739161b4c0116a9a2dccdfa1c492006f1"
    )

    MessageDigest.digest(message) shouldBe expectedDigest
  }
}
