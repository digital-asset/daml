// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value.json

import data.{Decimal, Ref, SortedLookupList, Time}
import value.json.{NavigatorModelAliases => model}
import value.TypedValueGenerators.{ValueAddend => VA, genAddend, genTypeAndValue}
import ApiCodecCompressed.{apiValueToJsValue, jsValueToApiValue}

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
      parsed <- Try(jsValueToApiValue(json, typ, typeLookup))
    } yield parsed
  }

  private def roundtrip(va: VA)(v: va.Inj[Cid]): Option[va.Inj[Cid]] =
    va.prj(jsValueToApiValue(apiValueToJsValue(va.inj(v)), va.t, typeLookup))

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
          roundtrip(va)(v) should ===(Some(v))
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
          roundtrip(va)(ool) should ===(Some(ool))
        }
      }

      "handle lists of optionals" in {
        val va = VA.optional(VA.optional(VA.list(VA.optional(VA.optional(VA.int64)))))
        import va.injshrink
        implicit val arbInj: Arbitrary[va.Inj[Cid]] = va.injarb(Arbitrary(genCid))
        forAll(minSuccessful(1000)) { v: va.Inj[Cid] =>
          roundtrip(va)(v) should ===(Some(v))
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

    def c(canonical: String, typ: VA)(expected: typ.Inj[Cid], alternates: String*)(
        implicit pos: source.Position) =
      (pos.lineNumber, canonical, typ, expected, alternates)

    object VAs {
      val ooi = VA.optional(VA.optional(VA.int64))
      val oooi = VA.optional(ooi)
    }

    val successes = Table(
      ("line#", "serialized", "type", "parsed", "alternates"),
      c("\"123\"", VA.contractId)("123"),
      c("\"42.0\"", VA.numeric)(Decimal assertFromString "42", "\"42\"" /*, "42", "42.0"*/ ),
      // c("2e3", VA.decimal)(Decimal assertFromString "2000"),
      c("\"2000.0\"", VA.numeric)(
        Decimal assertFromString "2000",
        "\"2000\"" /*, "2000", "2e3" */ ),
      c("\"0.3\"", VA.numeric)(
        Decimal assertFromString "0.3" /*, "\"0.30000000000000004\"", "0.30000000000000004", "0.3"*/ ),
      c("\"1990-11-09T04:30:23.123456Z\"", VA.timestamp)(
        Time.Timestamp assertFromString "1990-11-09T04:30:23.123456Z",
        "\"1990-11-09T04:30:23.1234569Z\""),
      c("\"42\"", VA.int64)(42),
      c("\"Alice\"", VA.party)(Ref.Party assertFromString "Alice"),
      c("{}", VA.unit)(()),
      c("\"2019-06-18\"", VA.date)(Time.Date assertFromString "2019-06-18"),
      c("\"abc\"", VA.text)("abc"),
      c("true", VA.bool)(true),
      c("[\"1\", \"2\", \"3\"]", VA.list(VA.int64))(Vector(1, 2, 3) /*, "[1, 2, 3]"*/ ),
      c("""{"a": "b", "c": "d"}""", VA.map(VA.text))(SortedLookupList(Map("a" -> "b", "c" -> "d"))),
      c("""{"None": {}}""", VAs.ooi)(None /*, "null"*/ ),
      c("""{"Some": {"None": {}}}""", VAs.ooi)(Some(None) /*, "[]"*/ ),
      c("""{"Some": {"Some": "42"}}""", VAs.ooi)(Some(Some(42)) /*, """[42]"""*/ ),
      c("""{"None": {}}""", VAs.oooi)(None /*, "null"*/ ),
      c("""{"Some": {"None": {}}}""", VAs.oooi)(Some(None) /*, "[]"*/ ),
      c("""{"Some": {"Some": {"None": {}}}}""", VAs.oooi)(Some(Some(None)) /*, "[[]]"*/ ),
      c("""{"Some": {"Some": {"Some": "42"}}}""", VAs.oooi)(Some(Some(Some(42))) /*, "[[42]]"*/ ),
    )

    "dealing with particular formats" should {
      "succeed in cases" in forEvery(successes) { (_, serialized, typ, expected, alternates) =>
        val json = serialized.parseJson
        val parsed = jsValueToApiValue(json, typ.t, typeLookup)
        typ.prj(parsed) should ===(Some(expected))
        apiValueToJsValue(parsed) should ===(json)
        val tAlternates = Table("alternate", alternates: _*)
        forEvery(tAlternates) { alternate =>
          val aJson = alternate.parseJson
          typ.prj(jsValueToApiValue(aJson, typ.t, typeLookup)) should ===(Some(expected))
        }
      }
    }
  }
}
