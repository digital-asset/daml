// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.meteringreport

import com.daml.platform.apiserver.meteringreport.HmacSha256.{Bytes, Key, generateKey, toBase64}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json._
import java.nio.charset.StandardCharsets

import org.scalatest.Inside.inside

class HmacSha256Spec extends AnyWordSpec with Matchers {

  HmacSha256.getClass.getName should {

    "generate serialize/deserialize bytes" in {
      val expected = Bytes("some string".getBytes)
      val json = expected.toJson.prettyPrint
      val actual = json.parseJson.convertTo[Bytes]
      actual shouldBe expected
    }
    "generate serialize/deserialize key" in {
      val expected = generateKey("test")
      val json = expected.toJson.prettyPrint
      val actual = json.parseJson.convertTo[Key]
      actual shouldBe expected
    }

    "compute MAC" in {
      val expected = "uFfrKWtNvoMl-GdCBrotl33cTFOqLeF8EjaooomUKOw="
      val key = MeteringReportKey.communityKey()
      inside(HmacSha256.compute(key, "some message".getBytes(StandardCharsets.UTF_8))) {
        case Right(mac) =>
          val actual = toBase64(mac)
          actual shouldBe expected
      }
    }

    "fail if key is invalid" in {
      val key = Key("invalid", Bytes(Array.empty), "")
      HmacSha256.compute(key, "some message".getBytes(StandardCharsets.UTF_8)).isLeft shouldBe true
    }

    "generate key" in {
      val expected = "test"
      HmacSha256.generateKey(expected).scheme shouldBe expected
    }

  }

}
