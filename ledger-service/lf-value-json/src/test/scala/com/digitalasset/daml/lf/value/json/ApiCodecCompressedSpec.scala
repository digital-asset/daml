// Copyright (c) 2019 The DAML Authors. All rights reserved.
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

    def cn(canonical: String, numerically: String, typ: VA)(
        expected: typ.Inj[Cid],
        alternates: String*)(implicit pos: source.Position) =
      (pos.lineNumber, canonical, numerically, typ, expected, alternates)

    def c(canonical: String, typ: VA)(expected: typ.Inj[Cid], alternates: String*)(
        implicit pos: source.Position) =
      cn(canonical, canonical, typ)(expected, alternates: _*)(pos)

    object VAs {
      val ooi = VA.optional(VA.optional(VA.int64))
      val oooi = VA.optional(ooi)
    }

    val numCodec = ApiCodecCompressed.copy(false, false)

    val successes = Table(
      ("line#", "serialized", "serializedNumerically", "type", "parsed", "alternates"),
      c("\"123\"", VA.contractId)("123"),
      cn("\"42.0\"", "42.0", VA.numeric(10))(
        Decimal assertFromString "42",
        "\"42\"",
        "42",
        "42.0",
        "\"+42\""),
      cn("\"2000.0\"", "2000", VA.numeric(10))(
        Decimal assertFromString "2000",
        "\"2000\"",
        "2000",
        "2e3"),
      cn("\"0.3\"", "0.3", VA.numeric(10))(
        Decimal assertFromString "0.3",
        "\"0.30000000000000004\"",
        "0.30000000000000004"),
      cn(
        "\"9999999999999999999999999999.9999999999\"",
        "9999999999999999999999999999.9999999999",
        VA.numeric(10))(Decimal assertFromString "9999999999999999999999999999.9999999999"),
      cn("\"0.1234512346\"", "0.1234512346", VA.numeric(10))(
        Decimal assertFromString "0.1234512346",
        "0.12345123455",
        "0.12345123465",
        "\"0.12345123455\"",
        "\"0.12345123465\""),
      cn("\"0.1234512345\"", "0.1234512345", VA.numeric(10))(
        Decimal assertFromString "0.1234512345",
        "0.123451234549",
        "0.12345123445001",
        "\"0.123451234549\"",
        "\"0.12345123445001\""),
      c("\"1990-11-09T04:30:23.123456Z\"", VA.timestamp)(
        Time.Timestamp assertFromString "1990-11-09T04:30:23.123456Z",
        "\"1990-11-09T04:30:23.1234569Z\""),
      c("\"1970-01-01T00:00:00Z\"", VA.timestamp)(Time.Timestamp assertFromLong 0),
      cn("\"42\"", "42", VA.int64)(42, "\"+42\""),
      cn("\"0\"", "0", VA.int64)(0, "-0", "\"+0\"", "\"-0\""),
      c("\"Alice\"", VA.party)(Ref.Party assertFromString "Alice"),
      c("{}", VA.unit)(()),
      c("\"2019-06-18\"", VA.date)(Time.Date assertFromString "2019-06-18"),
      c("\"abc\"", VA.text)("abc"),
      c("true", VA.bool)(true),
      cn("""["1", "2", "3"]""", "[1, 2, 3]", VA.list(VA.int64))(Vector(1, 2, 3)),
      c("""{"a": "b", "c": "d"}""", VA.map(VA.text))(SortedLookupList(Map("a" -> "b", "c" -> "d"))),
      cn("\"42\"", "42", VA.optional(VA.int64))(Some(42)),
      c("null", VA.optional(VA.int64))(None),
      c("null", VAs.ooi)(None),
      c("[]", VAs.ooi)(Some(None), "[null]"),
      cn("""["42"]""", "[42]", VAs.ooi)(Some(Some(42))),
      c("null", VAs.oooi)(None),
      c("[]", VAs.oooi)(Some(None), "[null]"),
      c("[[]]", VAs.oooi)(Some(Some(None)), "[[null]]"),
      cn("""[["42"]]""", "[[42]]", VAs.oooi)(Some(Some(Some(42)))),
    )

    val failures = Table(
      ("JSON", "type"),
      ("42.3", VA.int64),
      ("\"42.3\"", VA.int64),
      ("9223372036854775808", VA.int64),
      ("-9223372036854775809", VA.int64),
      ("\"garbage\"", VA.int64),
      ("\"   42 \"", VA.int64),
      ("\"1970-01-01T00:00:00\"", VA.timestamp),
      ("\"1970-01-01T00:00:00+01:00\"", VA.timestamp),
      ("\"1970-01-01T00:00:00+01:00[Europe/Paris]\"", VA.timestamp),
    )

    "dealing with particular formats" should {
      "succeed in cases" in forEvery(successes) {
        (_, serialized, serializedNumerically, typ, expected, alternates) =>
          val json = serialized.parseJson
          val numJson = serializedNumerically.parseJson
          val parsed = jsValueToApiValue(json, typ.t, typeLookup)
          jsValueToApiValue(numJson, typ.t, typeLookup) should ===(parsed)
          typ.prj(parsed) should ===(Some(expected))
          apiValueToJsValue(parsed) should ===(json)
          numCodec.apiValueToJsValue(parsed) should ===(numJson)
          val tAlternates = Table("alternate", alternates: _*)
          forEvery(tAlternates) { alternate =>
            val aJson = alternate.parseJson
            typ.prj(jsValueToApiValue(aJson, typ.t, typeLookup)) should ===(Some(expected))
          }
      }

      "fail in cases" in forEvery(failures) { (serialized, typ) =>
        val json = serialized.parseJson // we don't test *the JSON decoder*
        a[DeserializationException] shouldBe thrownBy {
          jsValueToApiValue(json, typ.t, typeLookup)
        }
      }
    }
  }
}
