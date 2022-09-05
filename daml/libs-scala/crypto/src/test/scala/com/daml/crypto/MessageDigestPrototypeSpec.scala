// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class MessageDigestPrototypeSpec extends AnyFlatSpec with Matchers {

  behavior of MessageDigestPrototype.getClass.getSimpleName

  it should "provide new instance of digest for SHA-256" in {
    val digest = MessageDigestPrototype.Sha256.newDigest
    val digest2 = MessageDigestPrototype.Sha256.newDigest
    digest should not be theSameInstanceAs(digest2)
  }

  it should "expose algorithm" in {
    MessageDigestPrototype.Sha256.algorithm shouldBe "SHA-256"
  }

  it should "perform a digest for the SHA-256 algorithm" in {
    val digest = MessageDigestPrototype.Sha256.newDigest
    val sha = digest.digest("Hello World".getBytes(StandardCharsets.UTF_8))
    new String(
      Base64.getEncoder.encode(sha),
      StandardCharsets.UTF_8,
    ) shouldBe "pZGm1Av0IEBKARczz7exkNYsZb8LzaMrV7J32a2fFG4="
  }
}
