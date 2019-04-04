// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen
import com.digitalasset.ledger.client.binding.encoding.{JsonLfTypeEncoding, LfEncodable}
import org.scalatest.{Matchers, WordSpec}
import com.digitalasset.sample.MyMain.{AlmostEnum, Enum}
import spray.json.{JsBoolean, JsObject, JsString, JsonFormat}

class JsonLfTypeEncodingSpec extends WordSpec with Matchers {
  val enumFormat: JsonFormat[Enum] = LfEncodable.encoding[Enum](JsonLfTypeEncoding)
  val almostEnumFormat: JsonFormat[AlmostEnum] =
    LfEncodable.encoding[AlmostEnum](JsonLfTypeEncoding)

  "JsonLfTypeEncodingSpec" should {
    "Encode nullary variants as enums" in {
      val e1s = enumFormat.write(Enum.E1(()))
      e1s shouldBe JsString("E1")
      val e2s = enumFormat.write(Enum.E2(()))
      e2s shouldBe JsString("E2")
      val e3s = enumFormat.write(Enum.E3(()))
      e3s shouldBe JsString("E3")
    }

    "Encode normal variants as single-element objects" in {
      val ae1s = almostEnumFormat.write(AlmostEnum.AE1(()))
      ae1s shouldBe JsObject("AE1" -> JsObject.empty)
      val ae2s = almostEnumFormat.write(AlmostEnum.AE2(true))
      ae2s shouldBe JsObject("AE2" -> JsBoolean(true))
      val ae3s = almostEnumFormat.write(AlmostEnum.AE3(()))
      ae3s shouldBe JsObject("AE3" -> JsObject.empty)
    }
  }
}
