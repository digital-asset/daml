// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value.json

import value.json.{NavigatorModelAliases => model}
import value.TypedValueGenerators.{ValueAddend => VA, genAddend, genTypeAndValue}
import ApiCodecCompressed.{apiValueToJsValue, jsValueToApiValue}
import ApiCodecCompressed.JsonImplicits.StringJsonFormat

import org.scalactic.source
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalacheck.{Arbitrary, Gen}
import spray.json._

import scala.util.{Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ApiCodecCompressedSpec
    extends WordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with TableDrivenPropertyChecks {

  /** XXX SC replace when TypedValueGenerators supports TypeCons */
  private val typeLookup: NavigatorModelAliases.DamlLfTypeLookup = _ => None

  /** Serializes the API value to JSON, then parses it back to an API value */
  private def serializeAndParse(
      value: model.ApiValue,
      typ: model.DamlLfType): Try[model.ApiValue] = {
    import ApiCodecCompressed.JsonImplicits._

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(jsValueToApiValue[String](json, typ, typeLookup))
    } yield parsed
  }

  type Cid = String
  private val genCid = Gen.zip(Gen.alphaChar, Gen.alphaStr) map { case (h, t) => h +: t }

  "API compressed JSON codec" when {

    "serializing and parsing a value" should {

      "work for arbitrary reference-free types" in forAll(
        genTypeAndValue(genCid),
        minSuccessful(100)) {
        case (typ, value) =>
          serializeAndParse(value, typ) shouldBe Success(value)
      }

      "work for many, many values in raw format" in forAll(genAddend, minSuccessful(100)) { va =>
        import va.injshrink
        implicit val arbInj: Arbitrary[va.Inj[Cid]] = va.injarb(Arbitrary(genCid))
        forAll(minSuccessful(20)) { v: va.Inj[Cid] =>
          va.prj(jsValueToApiValue(apiValueToJsValue(va.inj(v)), va.t, typeLookup)) should ===(
            Some(v))
        }
      }

      "handle nested optionals" in {
        val va = VA.optional(VA.optional(VA.int64))
        val cases = Table(
          "value",
          None,
          Some(None),
          Some(Some(42L)),
        )
        forEvery(cases) { ool =>
          va.prj(jsValueToApiValue(apiValueToJsValue(va.inj(ool)), va.t, typeLookup)) should ===(
            Some(ool))
        }
      }
      /*
      "work for EmptyRecord" in {
        serializeAndParse(C.emptyRecordV, C.emptyRecordTC) shouldBe Success(C.emptyRecordV)
      }
      "work for SimpleRecord" in {
        serializeAndParse(C.simpleRecordV, C.simpleRecordTC) shouldBe Success(C.simpleRecordV)
      }
      "work for SimpleVariant" in {
        serializeAndParse(C.simpleVariantV, C.simpleVariantTC) shouldBe Success(C.simpleVariantV)
      }
      "work for ComplexRecord" in {
        serializeAndParse(C.complexRecordV, C.complexRecordTC) shouldBe Success(C.complexRecordV)
      }
      "work for Tree" in {
        serializeAndParse(C.treeV, C.treeTC) shouldBe Success(C.treeV)
      }
      "work for Enum" in {
        serializeAndParse(C.redV, C.redTC) shouldBe Success(C.redV)
      }
     */
    }

    def c(serialized: String, typ: VA)(expected: typ.Inj[Cid])(implicit pos: source.Position) =
      (pos.lineNumber, serialized, typ, expected)

    val successes = Table(
      ("line#", "serialized", "type", "parsed"),
      c("\"abc\"", VA.text)("abc"),
      c("\"42\"", VA.int64)(42),
      // TODO SC c("[1, 2, 3]", VA.list(VA.int64))(Vector(1, 2, 3)),
    )

    "dealing with particular formats" should {
      "succeed in cases" in forEvery(successes) { (_, serialized, typ, expected) =>
        val json = serialized.parseJson
        val parsed = jsValueToApiValue[String](json, typ.t, typeLookup)
        typ.prj(parsed) should ===(Some(expected))
      }
    }
  }
}
