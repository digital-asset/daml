// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import com.digitalasset.ledger.client.binding.encoding.PrimitiveTypeGenerators.{
  primitiveDateGen,
  primitiveTimestampGen
}
import com.digitalasset.ledger.client.binding.{Primitive => P}
import org.scalactic.anyvals.PosInt
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import spray.json.JsValue

import scalaz.std.option._

class CustomJsonFormatsTest extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = PosInt(1000))

  "dateJsonFormat" should "read P.Date that write created" in forAll(primitiveDateGen) { d0 =>
    val sut = CustomJsonFormats.dateJsonFormat
    val jsValue: JsValue = sut.write(d0)
    val d1: P.Date = sut.read(jsValue)
    d1 shouldBe d0
  }

  "timestampJsonFormat" should "read P.Timestamp that write created" in forAll(
    primitiveTimestampGen) { t0 =>
    val sut = CustomJsonFormats.timestampJsonFormat
    val jsValue: JsValue = sut.write(t0)
    val t1: P.Timestamp = sut.read(jsValue)
    t1 shouldBe t0
  }

  behavior of "optionFormat"

  it should "render single layer like standard format" in {
    import spray.json._
    import CustomJsonFormats._
    none[Int].toJson shouldBe JsArray()
    some(42).toJson shouldBe JsArray(JsNumber(42))
  }

  it should "render double layer as hybrid" in {
    import spray.json._
    import CustomJsonFormats._
    none[Option[Int]].toJson shouldBe JsArray()
    some(none[Int]).toJson shouldBe JsArray(JsArray())
    some(some(42)).toJson shouldBe JsArray(JsArray(JsNumber(42)))
  }

  type QuadLayer = Option[Option[Option[Option[Int]]]]
  it should "be isomorphic for quad layer" in forAll { x: QuadLayer =>
    import spray.json._
    import CustomJsonFormats._
    x.toJson.convertTo[QuadLayer] shouldBe x
  }
}
