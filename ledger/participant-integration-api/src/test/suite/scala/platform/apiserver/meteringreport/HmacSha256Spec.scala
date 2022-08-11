// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.meteringreport

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json._

import java.nio.charset.StandardCharsets
import java.util.Base64

class HmacSha256Spec extends AnyWordSpec with Matchers {

  import HmacSha256._

  private def testCommunityKey(): Key = {
    val keyPath = "keys/community.json"
    val keyUrl = ClassLoader.getSystemResource(keyPath)
    val json = new String(keyUrl.openStream().readAllBytes(), StandardCharsets.UTF_8)
    println(json)
    json.parseJson.convertTo[Key]
  }

  "HmacSha256" should {
    "generate serialize/deserialize key" in {
      val expected = HmacSha256.generateKey()
      val json = expected.toJson.prettyPrint
      val actual = json.parseJson.convertTo[Key]
      actual shouldBe expected
    }
    "read community key from path" in {
      val expected = "iENTFX4g-fAvOBTXnGjIVfesNzmWFKpo_35zpUnXEsg="
      val key = testCommunityKey()
      key.algorithm shouldBe "HmacSHA256"
      val actual = key.encoded.toBase64
      actual shouldBe expected
    }
    "compute MAC" in {
      val expected = "uFfrKWtNvoMl-GdCBrotl33cTFOqLeF8EjaooomUKOw="
      val key = testCommunityKey()
      val Right(mac) = HmacSha256.compute(key, "some message".getBytes(StandardCharsets.UTF_8))
      val actual = Base64.getUrlEncoder.encodeToString(mac)
      actual shouldBe expected
    }
    "fail if key is invalid" in {
      val key = Key(Bytes(Array.empty), "")
      val Left(_) = HmacSha256.compute(key, "some message".getBytes(StandardCharsets.UTF_8))
    }

  }

}
