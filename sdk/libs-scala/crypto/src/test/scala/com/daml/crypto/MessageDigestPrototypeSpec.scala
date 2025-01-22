// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.security.Security
import java.util.Base64

class MessageDigestPrototypeSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  behavior of MessageDigestPrototype.getClass.getSimpleName

  override def beforeAll(): Unit = {
    val _ = Security.insertProviderAt(new BouncyCastleProvider, 1)
  }

  it should "expose algorithm" in {
    MessageDigestPrototype.Sha256.algorithm shouldBe "SHA-256"
    MessageDigestPrototype.KecCak256.algorithm shouldBe "KECCAK-256"
  }

  it should "provide new instance of digest for SHA-256" in {
    val digest = MessageDigestPrototype.Sha256.newDigest
    val digest2 = MessageDigestPrototype.Sha256.newDigest
    digest should not be theSameInstanceAs(digest2)
  }

  it should "perform a digest for the SHA-256 algorithm" in {
    val digest = MessageDigestPrototype.Sha256.newDigest
    val sha = digest.digest("Hello World".getBytes(StandardCharsets.UTF_8))
    new String(
      Base64.getEncoder.encode(sha),
      StandardCharsets.UTF_8,
    ) shouldBe "pZGm1Av0IEBKARczz7exkNYsZb8LzaMrV7J32a2fFG4="
  }

  it should "provide new instance of digest for KECCAK-256" in {
    val digest = MessageDigestPrototype.KecCak256.newDigest
    val digest2 = MessageDigestPrototype.KecCak256.newDigest
    digest should not be theSameInstanceAs(digest2)
  }

  it should "perform a digest for the KECCAK-256 algorithm" in {
    val digest = MessageDigestPrototype.KecCak256.newDigest
    val keccak = digest.digest("Hello World".getBytes(StandardCharsets.UTF_8))
    new String(
      Base64.getEncoder.encode(keccak),
      StandardCharsets.UTF_8,
    ) shouldBe "WS+nQ4ifx/kqwqN7sfW6Ha8qXIR0HKDgBh0kOi5nB7o="
  }
}
