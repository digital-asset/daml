// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value.json

import com.daml.bazeltools.BazelRunfiles._
import data.{Decimal, ImmArray, Ref, SortedLookupList, Time}
import value.json.{NavigatorModelAliases => model}
import value.TypedValueGenerators.{RNil, genAddend, genTypeAndValue, ValueAddend => VA}
import ApiCodecCompressed.{apiValueToJsValue, jsValueToApiValue}
import com.daml.ledger.service.MetadataReader
import org.scalactic.source
import org.scalatest.{Inside, Matchers, WordSpec}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalacheck.{Arbitrary, Gen}
import shapeless.{Coproduct => HSum}
import shapeless.record.{Record => HRecord}
import spray.json._
import scalaz.Order
import scalaz.std.string._
import scalaz.syntax.show._

import scala.util.{Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ApiCodecCompressedSpec
    extends WordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with TableDrivenPropertyChecks
    with Inside {

  import C.typeLookup

  private val dar = new java.io.File(rlocation("ledger-service/lf-value-json/JsonEncodingTest.dar"))
  require(dar.exists())

  private val darMetadata: MetadataReader.LfMetadata =
    MetadataReader
      .readFromDar(dar)
      .valueOr(e => fail(s"Cannot read metadata from $dar, error:" + e.shows))

  private val darTypeLookup: NavigatorModelAliases.DamlLfTypeLookup =
    MetadataReader.typeLookup(darMetadata)

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

  private object C /* based on navigator DamlConstants */ {
    import shapeless.syntax.singleton._
    val packageId0 = Ref.PackageId assertFromString "hash"
    val moduleName0 = Ref.ModuleName assertFromString "Module"
    def defRef(name: String) =
      Ref.Identifier(
        packageId0,
        Ref.QualifiedName(moduleName0, Ref.DottedName assertFromString name))
    val emptyRecordId = defRef("EmptyRecord")
    val (emptyRecordDDT, emptyRecordT) = VA.record(emptyRecordId, RNil)
    val simpleRecordId = defRef("SimpleRecord")
    val simpleRecordVariantSpec = 'fA ->> VA.text :: 'fB ->> VA.int64 :: RNil
    val (simpleRecordDDT, simpleRecordT) =
      VA.record(simpleRecordId, simpleRecordVariantSpec)
    val simpleRecordV: simpleRecordT.Inj[Cid] = HRecord(fA = "foo", fB = 100L)

    val simpleVariantId = defRef("SimpleVariant")
    val (simpleVariantDDT, simpleVariantT) =
      VA.variant(simpleVariantId, simpleRecordVariantSpec)
    val simpleVariantV = HSum[simpleVariantT.Inj[Cid]]('fA ->> "foo")

    val complexRecordId = defRef("ComplexRecord")
    val (complexRecordDDT, complexRecordT) =
      VA.record(
        complexRecordId,
        'fText ->> VA.text
          :: 'fBool ->> VA.bool
          :: 'fDecimal ->> VA.numeric(Decimal.scale)
          :: 'fUnit ->> VA.unit
          :: 'fInt64 ->> VA.int64
          :: 'fParty ->> VA.party
          :: 'fContractId ->> VA.contractId
          :: 'fListOfText ->> VA.list(VA.text)
          :: 'fListOfUnit ->> VA.list(VA.unit)
          :: 'fDate ->> VA.date
          :: 'fTimestamp ->> VA.timestamp
          :: 'fOptionalText ->> VA.optional(VA.text)
          :: 'fOptionalUnit ->> VA.optional(VA.unit)
          :: 'fOptOptText ->> VA.optional(VA.optional(VA.text))
          :: 'fMap ->> VA.map(VA.int64)
          :: 'fVariant ->> simpleVariantT
          :: 'fRecord ->> simpleRecordT
          :: RNil
      )
    val complexRecordV: complexRecordT.Inj[Cid] =
      HRecord(
        fText = "foo",
        fBool = true,
        fDecimal = Decimal assertFromString "100",
        fUnit = (),
        fInt64 = 100L,
        fParty = Ref.Party assertFromString "BANK1",
        fContractId = "C0",
        fListOfText = Vector("foo", "bar"),
        fListOfUnit = Vector((), ()),
        fDate = Time.Date assertFromString "2019-01-28",
        fTimestamp = Time.Timestamp assertFromString "2019-01-28T12:44:33.22Z",
        fOptionalText = None,
        fOptionalUnit = Some(()),
        fOptOptText = Some(Some("foo")),
        fMap = SortedLookupList(Map("1" -> 1L, "2" -> 2L, "3" -> 3L)),
        fVariant = simpleVariantV,
        fRecord = simpleRecordV
      )

    val colorId = defRef("Color")
    val (colorGD, colorGT) =
      VA.enum(colorId, Seq("Red", "Green", "Blue") map Ref.Name.assertFromString)

    val typeLookup: NavigatorModelAliases.DamlLfTypeLookup =
      Map(
        emptyRecordId -> emptyRecordDDT,
        simpleRecordId -> simpleRecordDDT,
        simpleVariantId -> simpleVariantDDT,
        complexRecordId -> complexRecordDDT,
        colorId -> colorGD).lift
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
        implicit val arbInj: Arbitrary[va.Inj[Cid]] = va.injarb(Arbitrary(genCid), Order[Cid])
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
        implicit val arbInj: Arbitrary[va.Inj[Cid]] = va.injarb(Arbitrary(genCid), Order[Cid])
        forAll(minSuccessful(1000)) { v: va.Inj[Cid] =>
          roundtrip(va)(v) should ===(Some(v))
        }
      }

      def cr(typ: VA)(v: typ.Inj[Cid]) =
        (typ, v: Any, typ.inj(v))

      val roundtrips = Table(
        ("type", "original value", "DAML value"),
        cr(C.emptyRecordT)(HRecord()),
        cr(C.simpleRecordT)(C.simpleRecordV),
        cr(C.simpleVariantT)(C.simpleVariantV),
        cr(C.complexRecordT)(C.complexRecordV),
      )
      "work for records and variants" in forAll(roundtrips) { (typ, origValue, damlValue) =>
        typ.prj(jsValueToApiValue(apiValueToJsValue(damlValue), typ.t, typeLookup)) should ===(
          Some(origValue))
      }
      /*
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
      cn("\"42.0\"", "42.0", VA.numeric(Decimal.scale))(
        Decimal assertFromString "42",
        "\"42\"",
        "42",
        "42.0",
        "\"+42\""),
      cn("\"2000.0\"", "2000", VA.numeric(Decimal.scale))(
        Decimal assertFromString "2000",
        "\"2000\"",
        "2000",
        "2e3"),
      cn("\"0.3\"", "0.3", VA.numeric(Decimal.scale))(
        Decimal assertFromString "0.3",
        "\"0.30000000000000004\"",
        "0.30000000000000004"),
      cn(
        "\"9999999999999999999999999999.9999999999\"",
        "9999999999999999999999999999.9999999999",
        VA.numeric(Decimal.scale))(
        Decimal assertFromString "9999999999999999999999999999.9999999999"),
      cn("\"0.1234512346\"", "0.1234512346", VA.numeric(Decimal.scale))(
        Decimal assertFromString "0.1234512346",
        "0.12345123455",
        "0.12345123465",
        "\"0.12345123455\"",
        "\"0.12345123465\""),
      cn("\"0.1234512345\"", "0.1234512345", VA.numeric(Decimal.scale))(
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
      cn("""{"fA": "foo", "fB": "100"}""", """{"fA": "foo", "fB": 100}""", C.simpleRecordT)(
        C.simpleRecordV),
      c("""{"tag": "fA", "value": "foo"}""", C.simpleVariantT)(C.simpleVariantV),
      c("\"Green\"", C.colorGT)(
        C.colorGT get Ref.Name.assertFromString("Green") getOrElse sys.error("impossible")),
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

    import com.daml.lf.value.{Value => LfValue}
    import ApiCodecCompressed.JsonImplicits._

    val packageId: Ref.PackageId = mustBeOne(
      MetadataReader.typeByName(darMetadata)(
        Ref.QualifiedName.assertFromString("JsonEncodingTest:Foo")))._1

    val bazRecord = LfValue.ValueRecord[String](
      None,
      ImmArray(Some(Ref.Name.assertFromString("baz")) -> LfValue.ValueText("text abc"))
    )

    val bazVariant = LfValue.ValueVariant[String](
      None,
      Ref.Name.assertFromString("Baz"),
      bazRecord
    )

    val quxVariant = LfValue.ValueVariant[String](
      None,
      Ref.Name.assertFromString("Qux"),
      LfValue.ValueUnit
    )

    val fooId =
      Ref.Identifier(packageId, Ref.QualifiedName.assertFromString("JsonEncodingTest:Foo"))

    val bazRecordId =
      Ref.Identifier(packageId, Ref.QualifiedName.assertFromString("JsonEncodingTest:BazRecord"))

    "dealing with LF Variant" should {
      "encode Foo/Baz to JSON" in {
        val writer = implicitly[spray.json.JsonWriter[LfValue[String]]]
        (writer.write(bazVariant): JsValue) shouldBe ("""{"tag":"Baz", "value":{"baz":"text abc"}}""".parseJson: JsValue)
      }

      "decode Foo/Baz from JSON" in {
        val actualValue: LfValue[String] = jsValueToApiValue(
          """{"tag":"Baz", "value":{"baz":"text abc"}}""".parseJson,
          fooId,
          darTypeLookup
        )

        val expectedValueWithIds: LfValue.ValueVariant[String] =
          bazVariant.copy(tycon = Some(fooId), value = bazRecord.copy(tycon = Some(bazRecordId)))

        actualValue shouldBe expectedValueWithIds
      }

      "encode Foo/Qux to JSON" in {
        val writer = implicitly[spray.json.JsonWriter[LfValue[String]]]
        (writer.write(quxVariant): JsValue) shouldBe ("""{"tag":"Qux", "value":{}}""".parseJson: JsValue)
      }

      "fail decoding Foo/Qux from JSON if 'value' filed is missing" in {
        assertThrows[spray.json.DeserializationException] {
          jsValueToApiValue(
            """{"tag":"Qux"}""".parseJson,
            fooId,
            darTypeLookup
          )
        }
      }

      "decode Foo/Qux (empty value) from JSON" in {
        val actualValue: LfValue[String] = jsValueToApiValue(
          """{"tag":"Qux", "value":{}}""".parseJson,
          fooId,
          darTypeLookup
        )

        val expectedValueWithIds: LfValue.ValueVariant[String] =
          quxVariant.copy(tycon = Some(fooId))

        actualValue shouldBe expectedValueWithIds
      }
    }

    "dealing with Contract Key" should {

      "decode type Key = Party from JSON" in {
        val templateDef: iface.InterfaceType.Template = mustBeOne(
          MetadataReader.templateByName(darMetadata)(
            Ref.QualifiedName.assertFromString("JsonEncodingTest:KeyedByParty")))._2

        val keyType = templateDef.template.key.getOrElse(fail("Expected a key, got None"))
        val expectedValue: LfValue[String] = LfValue.ValueParty(Ref.Party.assertFromString("Alice"))

        jsValueToApiValue(JsString("Alice"), keyType, darTypeLookup) shouldBe expectedValue
      }

      "decode type Key = (Party, Int) from JSON" in {
        val templateDef: iface.InterfaceType.Template = mustBeOne(
          MetadataReader.templateByName(darMetadata)(
            Ref.QualifiedName.assertFromString("JsonEncodingTest:KeyedByPartyInt")))._2

        val tuple2Name = Ref.QualifiedName.assertFromString("DA.Types:Tuple2")
        val daTypesPackageId: Ref.PackageId =
          mustBeOne(MetadataReader.typeByName(darMetadata)(tuple2Name))._1

        val keyType = templateDef.template.key.getOrElse(fail("Expected a key, got None"))

        val expectedValue: LfValue[String] = LfValue.ValueRecord(
          Some(Ref.Identifier(daTypesPackageId, tuple2Name)),
          ImmArray(
            Some(Ref.Name.assertFromString("_1")) -> LfValue.ValueParty(
              Ref.Party.assertFromString("Alice")),
            Some(Ref.Name.assertFromString("_2")) -> LfValue.ValueInt64(123)
          )
        )

        jsValueToApiValue("""["Alice", 123]""".parseJson, keyType, darTypeLookup) shouldBe expectedValue
      }

      "decode type Key = (Party, (Int, Foo, BazRecord)) from JSON" in {
        val templateDef: iface.InterfaceType.Template = mustBeOne(
          MetadataReader.templateByName(darMetadata)(
            Ref.QualifiedName.assertFromString("JsonEncodingTest:KeyedByVariantAndRecord")))._2

        val keyType = templateDef.template.key.getOrElse(fail("Expected a key, got None"))

        val actual: LfValue[String] = jsValueToApiValue(
          """["Alice", [11, {"tag": "Bar", "value": 123}, {"baz": "baz text"}]]""".parseJson,
          keyType,
          darTypeLookup
        )

        inside(actual) {
          case LfValue.ValueRecord(Some(id2), ImmArray((_, party), (_, record2))) =>
            id2.qualifiedName.name shouldBe Ref.DottedName.assertFromString("Tuple2")
            party shouldBe LfValue.ValueParty(Ref.Party.assertFromString("Alice"))

            inside(record2) {
              case LfValue.ValueRecord(Some(id3), ImmArray((_, age), _, _)) =>
                id3.qualifiedName.name shouldBe Ref.DottedName.assertFromString("Tuple3")
                age shouldBe LfValue.ValueInt64(11)
            }
        }
      }
    }
  }

  private def mustBeOne[A](as: Seq[A]): A = as match {
    case Seq(x) => x
    case xs @ _ => sys.error(s"Expected exactly one element, got: $xs")
  }
}
