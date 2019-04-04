// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import com.digitalasset.ledger.client.binding.encoding.JdbcJsonTypeCodecs.ListOptionMapToJsonCodec
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FreeSpec, Matchers}
import spray.json._

class ListOptionToJsonCodecTest extends FreeSpec with Matchers with GeneratorDrivenPropertyChecks {

  import CustomJsonFormats._

  val sut = new ListOptionMapToJsonCodec(CustomJsonFormats)

  "Option should encode to an expected JSON" in {
    testOptionEncoding(None: Option[Long], JsArray())
    testOptionEncoding(Some(123L), JsArray(JsNumber(123L)))
    testOptionEncoding(Some(Some(1L)): Option[Option[Long]], JsArray(JsArray(JsNumber(1L))))
    testOptionEncoding(Some(None: Option[Long]), JsArray(JsArray()))
  }

  def testOptionEncoding[A: JsonFormat](a: Option[A], expectedJsValue: JsValue): Unit = {
    val ev0: JsonFormat[A] = implicitly
    val ev1: JsonLfTypeEncoding.Out[A] = JsonLfTypeEncoding.OutJson(ev0)
    val jsValue: JsValue = sut.encodeOption(a)(ev1)
    jsValue shouldBe expectedJsValue
    ()
  }

  "Option to JSON Codec should be symmetric" in {
    testOptionEncodingDecoding(Some(Some(1L)): Option[Option[Long]])
    testOptionEncodingDecoding(Some(1L))
    testOptionEncodingDecoding(None: Option[Long])
    testOptionEncodingDecoding(Some(None: Option[Long]))
  }

  def testOptionEncodingDecoding[A: JsonFormat](a: Option[A]): Unit = {
    val ev0: JsonFormat[A] = implicitly
    val ev1: JsonLfTypeEncoding.Out[A] = JsonLfTypeEncoding.OutJson(ev0)
    val jsValue: JsValue = sut.encodeOption(a)(ev1)
    val b: Option[A] = sut.decodeOption(jsValue)(ev1)
    a shouldBe b
    ()
  }
}
