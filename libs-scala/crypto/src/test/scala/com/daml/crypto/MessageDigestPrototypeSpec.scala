// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.Charset
import java.util.Base64

class MessageDigestPrototypeSpec extends AnyFlatSpec with Matchers {

  behavior of MessageDigestPrototype.getClass.getSimpleName

  it should "provide new instance of digest for SHA-256" in {
    val digest = MessageDigestPrototype.SHA_256.newDigest
    val digest2 = MessageDigestPrototype.SHA_256.newDigest
    digest should not be digest2
  }

  it should "expose algorithm" in {
    MessageDigestPrototype.SHA_256.algorithm shouldBe "SHA-256"
  }

  it should "work for SHA-256" in {
    val digest = MessageDigestPrototype.SHA_256.newDigest
    val charset = Charset.forName("UTF-8")
    val sha = digest.digest("Hello World".getBytes(charset))
    new String(
      Base64.getEncoder.encode(sha),
      charset,
    ) shouldBe "pZGm1Av0IEBKARczz7exkNYsZb8LzaMrV7J32a2fFG4="
  }
}
