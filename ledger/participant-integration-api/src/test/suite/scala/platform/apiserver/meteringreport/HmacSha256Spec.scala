// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.meteringreport

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json._

import java.nio.charset.StandardCharsets
import java.util.Base64

class HmacSha256Spec extends AnyWordSpec with Matchers {

  def assert(value: JsValue, expected: String): Assertion = {
    val actual = Jcs.serialize(value)
    actual shouldBe Right(expected)
  }

  HmacSha256.getClass.getName should {
    "generate serialize/deserialize key" in {
      val expected = HmacSha256.generateKey()
      val json = expected.toJson.prettyPrint
      val actual = json.parseJson.convertTo[HmacSha256.Key]
      actual shouldBe expected
    }
    "read community key from path" in {
      val keyPath = "keys/community.json"
      val keyUrl = ClassLoader.getSystemResource(keyPath)
      val json = new String(keyUrl.openStream().readAllBytes(), StandardCharsets.UTF_8)
      json.parseJson.convertTo[HmacSha256.Key].algorithm shouldBe "HmacSHA256"
    }
    "should compute MAC" in {
      val expected = "e54e-mOu-biHFTr45Np7AEnPHVAc9uBxe0GKF-3cjz0="
      val key = HmacSha256.generateKey()
      val mac = HmacSha256.compute(key, "some message".getBytes(StandardCharsets.UTF_8))
      val actual = Base64.getUrlEncoder.encodeToString(mac)
      actual shouldBe expected
    }
  }

}
