// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    val digest = MacPrototype.HmacSHA_256.newMac
    val digest2 = MacPrototype.HmacSHA_256.newMac
    digest should not be digest2
  }

  it should "expose algorithm" in {
    MacPrototype.HmacSHA_256.algorithm shouldBe "HmacSHA256"
  }

  it should "work for HmacSHA256" in {
    val key = "Hello"
    val prototype = MacPrototype.HmacSHA_256
    val mac = prototype.newMac
    mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), prototype.algorithm))
    val sha = mac.doFinal("Hello World".getBytes(StandardCharsets.UTF_8))
    new String(
      Base64.getEncoder.encode(sha),
      StandardCharsets.UTF_8,
    ) shouldBe "Y0PTLXgtpY9zSmzT6w2U48JcDGx7G7pRyHTRCIE/Pm0="
  }
}
