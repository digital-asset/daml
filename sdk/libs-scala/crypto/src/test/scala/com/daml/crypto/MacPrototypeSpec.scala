// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.crypto.spec.SecretKeySpec

class MacPrototypeSpec extends AnyFlatSpec with Matchers {

  behavior of MacPrototype.getClass.getSimpleName

  it should "provide new instance of digest for HmacSHA256" in {
    val mac = MacPrototype.HmacSha256.newMac
    val mac2 = MacPrototype.HmacSha256.newMac
    mac should not be theSameInstanceAs(mac2)
  }

  it should "expose algorithm" in {
    MacPrototype.HmacSha256.algorithm shouldBe "HmacSHA256"
  }

  it should "perform encoding for the `HmacSHA256` algorithm" in {
    val key = "Hello"
    val prototype = MacPrototype.HmacSha256
    val mac = prototype.newMac
    mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), prototype.algorithm))
    val sha = mac.doFinal("Hello World".getBytes(StandardCharsets.UTF_8))
    new String(
      Base64.getEncoder.encode(sha),
      StandardCharsets.UTF_8,
    ) shouldBe "Y0PTLXgtpY9zSmzT6w2U48JcDGx7G7pRyHTRCIE/Pm0="
  }
}
