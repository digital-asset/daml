// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.meteringreport

import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.platform.apiserver.meteringreport.Jcs.{
  MaximumSupportedAbsSize,
  serialize,
}
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReport.{
  ApplicationReport,
  ParticipantReport,
  Request,
}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue, enrichAny}

class JcsSpec extends AnyWordSpec with Matchers {

  private val uno: JsValue = JsNumber(1)
  private val badNumber: JsValue = JsNumber(1.2)

  private def assert(value: JsValue, expected: String): Assertion = {
    val actual = Jcs.serialize(value)
    actual shouldBe Right(expected)
  }

  "Jcs" should {
    "serialize JsString" in {
      assert(JsString("a\tb\nc"), "\"a\\tb\\nc\"")
    }
    "serialize JsBoolean" in {
      assert(JsBoolean(true), "true")
      assert(JsBoolean(false), "false")
    }
    "serialize JsNull" in {
      assert(JsNull, "null")
    }
    "serialize JsNumber(0)" in {
      assert(JsNumber(0), "0")
    }
    "serialize JsNumber < 2^52" in {
      assert(JsNumber(MaximumSupportedAbsSize - 1), "4503599627370495")
      assert(JsNumber(-MaximumSupportedAbsSize + 1), "-4503599627370495")
    }
    "not serialize JsNumber >= 2^52" in {
      serialize(JsNumber(MaximumSupportedAbsSize)).isLeft shouldBe true
      serialize(JsNumber(-MaximumSupportedAbsSize)).isLeft shouldBe true
    }
    "not serialize Decimals" in {
      serialize(badNumber).isLeft shouldBe true
    }
    "serialize JsNumber without scientific notation" in {
      assert(JsNumber(1000000000000000d), "1000000000000000")
      assert(JsNumber(-1000000000000000d), "-1000000000000000")
    }
    "serialize JsArray" in {
      assert(JsArray(Vector(true, false).map(b => JsBoolean(b))), "[true,false]")
    }
    "not serialize a JsArray containing an invalid number" in {
      serialize(JsArray(Vector(badNumber))).isLeft shouldBe true
    }
    "serialize JsObject in key order" in {
      assert(JsObject(Map("b" -> uno, "c" -> uno, "a" -> uno)), "{\"a\":1,\"b\":1,\"c\":1}")
    }
    "not serialize JsObject containing invalid number" in {
      serialize(JsObject(Map("n" -> badNumber))).isLeft shouldBe true
    }
    "serialize report" in {
      val application = Ref.ApplicationId.assertFromString("a0")
      val from = Timestamp.assertFromString("2022-01-01T00:00:00Z")
      val to = Timestamp.assertFromString("2022-01-01T00:00:00Z")
      val report = ParticipantReport(
        participant = Ref.ParticipantId.assertFromString("p0"),
        request = Request(from, Some(to), Some(application)),
        `final` = false,
        applications = Seq(ApplicationReport(application, 272)),
        check = None,
      )
      val reportJson: JsValue = report.toJson
      assert(
        reportJson,
        expected = "{" +
          "\"applications\":[{" +
          "\"application\":\"a0\"," +
          "\"events\":272" +
          "}]," +
          "\"final\":false," +
          "\"participant\":\"p0\"," +
          "\"request\":{" +
          "\"application\":\"a0\"," +
          "\"from\":\"2022-01-01T00:00:00Z\"," +
          "\"to\":\"2022-01-01T00:00:00Z\"" +
          "}" +
          "}",
      )
    }
  }

}
