// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value.json

import value.json.{NavigatorModelAliases => model}
import value.TypedValueGenerators.{genTypeAndValue, genAddend}

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.{Arbitrary, Gen}

import scala.util.{Success, Try}

class ApiCodecCompressedSpec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {

  /** XXX SC replace when TypedValueGenerators supports TypeCons */
  private val typeLookup: NavigatorModelAliases.DamlLfTypeLookup = _ => None

  /** Serializes the API value to JSON, then parses it back to an API value */
  private def serializeAndParse(
      value: model.ApiValue,
      typ: model.DamlLfType): Try[model.ApiValue] = {
    import ApiCodecCompressed.JsonImplicits._
    import spray.json._

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(ApiCodecCompressed.jsValueToApiValue(json, typ, typeLookup))
    } yield parsed
  }

  type Cid = String
  private val genCid = Gen.zip(Gen.alphaChar, Gen.alphaStr) map { case (h, t) => h +: t }

  "API compressed JSON codec" when {

    "serializing and parsing a value" should {

      "work for arbitrary reference-free types" in forAll(genTypeAndValue(genCid)) {
        case (typ, value) =>
          serializeAndParse(value, typ) shouldBe Success(value)
      }

      "work for many, many values in raw format" in forAll(genAddend) { va =>
        import va.injshrink
        implicit val arbInj: Arbitrary[va.Inj[Cid]] = Arbitrary(va.injgen(genCid))
        forAll { v: va.Inj[Cid] =>
          va.prj(
            ApiCodecCompressed.jsValueToApiValue(
              ApiCodecCompressed.apiValueToJsValue(va.inj(v)),
              va.t,
              typeLookup)) should ===(Some(v))
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
  }
}
