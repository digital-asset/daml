// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.daml.lf.value.json

import com.digitalasset.canton.daml.lf.value.json.NavigatorModelAliases as model
import com.digitalasset.canton.ledger.service.MetadataReader
import com.digitalasset.canton.util.JarResourceUtils
import com.digitalasset.daml.lf.data.{ImmArray, Numeric, Ref, SortedLookupList, Time}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.{
  ValueAddend as VA,
  genAddend,
  genTypeAndValue,
}
import com.digitalasset.daml.lf.value.test.ValueGenerators.coidGen
import org.scalacheck.Arbitrary
import org.scalactic.source
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scalaz.syntax.show.*
import shapeless.record.Record as HRecord
import shapeless.{Coproduct as HSum, HNil}
import spray.json.*

import java.time.Instant
import scala.annotation.nowarn
import scala.util.{Success, Try}

import ApiCodecCompressed.{apiValueToJsValue, jsValueToApiValue}

abstract class ApiCodecCompressedSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with Inside {

  def darPath: String

  import C.typeLookup

  protected implicit val cidArb: Arbitrary[ContractId] = Arbitrary(coidGen)

  private val dar = JarResourceUtils.resourceFile(darPath)
  require(dar.exists())

  protected val darMetadata: MetadataReader.LfMetadata =
    MetadataReader
      .readFromDar(dar)
      .valueOr(e => fail(s"Cannot read metadata from $dar, error:" + e.shows))

  protected val darTypeLookup: NavigatorModelAliases.DamlLfTypeLookup =
    MetadataReader.typeLookup(darMetadata)

  /** Serializes the API value to JSON, then parses it back to an API value */
  protected def serializeAndParse(
      value: model.ApiValue,
      typ: model.DamlLfType,
  ): Try[model.ApiValue] = {
    import ApiCodecCompressed.JsonImplicits.*

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(jsValueToApiValue(json, typ, typeLookup))
    } yield parsed
  }

  protected def roundtrip(va: VA)(v: va.Inj): Option[va.Inj] =
    va.prj(jsValueToApiValue(apiValueToJsValue(va.inj(v)), va.t, typeLookup))

  protected val decimalScale = Numeric.Scale.assertFromInt(10)

  protected object C /* based on navigator DamlConstants */ {
    import shapeless.syntax.singleton.*
    val packageId0 = Ref.PackageId assertFromString "hash"
    val moduleName0 = Ref.ModuleName assertFromString "Module"
    def defRef(name: String) =
      Ref.Identifier(
        packageId0,
        Ref.QualifiedName(moduleName0, Ref.DottedName assertFromString name),
      )
    val emptyRecordId = defRef("EmptyRecord")
    val (emptyRecordDDT, emptyRecordT) = VA.record(emptyRecordId, HNil)
    val simpleRecordId = defRef("SimpleRecord")
    val simpleRecordVariantSpec = HRecord(fA = VA.text, fB = VA.int64)
    val (simpleRecordDDT, simpleRecordT) =
      VA.record(simpleRecordId, simpleRecordVariantSpec)
    val simpleRecordV: simpleRecordT.Inj = HRecord(fA = "foo", fB = 100L)

    val simpleVariantId = defRef("SimpleVariant")
    val (simpleVariantDDT, simpleVariantT) =
      VA.variant(simpleVariantId, simpleRecordVariantSpec)
    val simpleVariantV = HSum[simpleVariantT.Inj](Symbol("fA") ->> "foo")

    val complexRecordId = defRef("ComplexRecord")
    val (complexRecordDDT, complexRecordT) =
      VA.record(
        complexRecordId,
        HRecord(
          fText = VA.text,
          fBool = VA.bool,
          fDecimal = VA.numeric(decimalScale),
          fUnit = VA.unit,
          fInt64 = VA.int64,
          fParty = VA.party,
          fContractId = VA.contractId,
          fListOfText = VA.list(VA.text),
          fListOfUnit = VA.list(VA.unit),
          fDate = VA.date,
          fTimestamp = VA.timestamp,
          fOptionalText = VA.optional(VA.text),
          fOptionalUnit = VA.optional(VA.unit),
          fOptOptText = VA.optional(VA.optional(VA.text)),
          fMap = VA.map(VA.int64),
          fVariant = simpleVariantT,
          fRecord = simpleRecordT,
        ),
      )
    @nowarn("msg=dubious usage of method asInstanceOf with unit value")
    val complexRecordV: complexRecordT.Inj =
      HRecord(
        fText = "foo",
        fBool = true,
        fDecimal = Numeric assertFromString "100.0000000000",
        fUnit = (),
        fInt64 = 100L,
        fParty = Ref.Party assertFromString "BANK1",
        fContractId = ContractId.assertFromString("00" + "00" * 32 + "c0"),
        fListOfText = Vector("foo", "bar"),
        fListOfUnit = Vector((), ()),
        fDate = Time.Date assertFromString "2019-01-28",
        fTimestamp = Time.Timestamp.assertFromInstant(Instant.parse("2019-01-28T12:44:33.22Z")),
        fOptionalText = None,
        fOptionalUnit = Some(()),
        fOptOptText = Some(Some("foo")),
        fMap = SortedLookupList(Map("1" -> 1L, "2" -> 2L, "3" -> 3L)),
        fVariant = simpleVariantV,
        fRecord = simpleRecordV,
      )

    val colorId = defRef("Color")
    val (colorGD, colorGT) =
      VA.enumeration(colorId, Seq("Red", "Green", "Blue") map Ref.Name.assertFromString)

    val typeLookup: NavigatorModelAliases.DamlLfTypeLookup =
      Map(
        emptyRecordId -> emptyRecordDDT,
        simpleRecordId -> simpleRecordDDT,
        simpleVariantId -> simpleVariantDDT,
        complexRecordId -> complexRecordDDT,
        colorId -> colorGD,
      ).lift
  }

  protected def mustBeOne[A](as: Seq[A]): A = as match {
    case Seq(x) => x
    case xs @ _ => sys.error(s"Expected exactly one element, got: $xs")
  }
}

class ApiCodecCompressedSpecStable extends ApiCodecCompressedSpec {

  import C.typeLookup

  override def darPath: String = "JsonEncodingTest.dar"

  "API compressed JSON codec" when {

    "serializing and parsing a value" should {

      "work for arbitrary reference-free types" in forAll(
        genTypeAndValue(coidGen),
        minSuccessful(100),
      ) { case (typ, value) =>
        serializeAndParse(value, typ) shouldBe Success(value)
      }

      "work for many, many values in raw format" in forAll(genAddend, minSuccessful(100)) { va =>
        import va.injshrink
        implicit val arbInj: Arbitrary[va.Inj] = va.injarb
        forAll(minSuccessful(20)) { (v: va.Inj) =>
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
        implicit val arbInj: Arbitrary[va.Inj] = va.injarb
        forAll(minSuccessful(1000)) { (v: va.Inj) =>
          roundtrip(va)(v) should ===(Some(v))
        }
      }

      def cr(typ: VA)(v: typ.Inj) =
        (typ, v: Any, typ.inj(v))

      val roundtrips = Table(
        ("type", "original value", "Daml value"),
        cr(C.emptyRecordT)(HRecord()),
        cr(C.simpleRecordT)(C.simpleRecordV),
        cr(C.simpleVariantT)(C.simpleVariantV),
        cr(C.complexRecordT)(C.complexRecordV),
      )
      "work for records and variants" in forAll(roundtrips) { (typ, origValue, damlValue) =>
        typ.prj(jsValueToApiValue(apiValueToJsValue(damlValue), typ.t, typeLookup)) should ===(
          Some(origValue)
        )
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
        expected: typ.Inj,
        alternates: String*
    )(implicit pos: source.Position) =
      (pos.lineNumber, canonical, numerically, typ, expected, alternates)

    def c(canonical: String, typ: VA)(expected: typ.Inj, alternates: String*)(implicit
        pos: source.Position
    ) =
      cn(canonical, canonical, typ)(expected, alternates*)(pos)

    object VAs {
      val ooi = VA.optional(VA.optional(VA.int64))
      val oooi = VA.optional(ooi)
    }

    val numCodec = ApiCodecCompressed.copy(false, false)

    @nowarn("cat=lint-infer-any")
    val successes = Table(
      ("line#", "serialized", "serializedNumerically", "type", "parsed", "alternates"),
      c(
        "\"0000000000000000000000000000000000000000000000000000000000000000000123\"",
        VA.contractId,
      )(
        ContractId.assertFromString(
          "0000000000000000000000000000000000000000000000000000000000000000000123"
        )
      ),
      cn("\"42.0\"", "42.0", VA.numeric(decimalScale))(
        Numeric assertFromString "42.0000000000",
        "\"42\"",
        "42",
        "42.0",
        "\"+42\"",
      ),
      cn("\"2000.0\"", "2000", VA.numeric(decimalScale))(
        Numeric assertFromString "2000.0000000000",
        "\"2000\"",
        "2000",
        "2e3",
      ),
      cn("\"0.3\"", "0.3", VA.numeric(decimalScale))(
        Numeric assertFromString "0.3000000000",
        "\"0.30000000000000004\"",
        "0.30000000000000004",
      ),
      cn(
        "\"9999999999999999999999999999.9999999999\"",
        "9999999999999999999999999999.9999999999",
        VA.numeric(decimalScale),
      )(Numeric assertFromString "9999999999999999999999999999.9999999999"),
      cn("\"0.1234512346\"", "0.1234512346", VA.numeric(decimalScale))(
        Numeric assertFromString "0.1234512346",
        "0.12345123455",
        "0.12345123465",
        "\"0.12345123455\"",
        "\"0.12345123465\"",
      ),
      cn("\"0.1234512345\"", "0.1234512345", VA.numeric(decimalScale))(
        Numeric assertFromString "0.1234512345",
        "0.123451234549",
        "0.12345123445001",
        "\"0.123451234549\"",
        "\"0.12345123445001\"",
      ),
      c("\"1990-11-09T04:30:23.123456Z\"", VA.timestamp)(
        Time.Timestamp.assertFromInstant(Instant.parse("1990-11-09T04:30:23.123456Z")),
        "\"1990-11-09T04:30:23.1234569Z\"",
      ),
      c("\"1970-01-01T00:00:00Z\"", VA.timestamp)(Time.Timestamp assertFromLong 0),
      // Ensure ISO 8601 timestamps with offsets are successfully parsed by comparing to (epoch - 1 hour)
      c("\"1969-12-31T23:00:00Z\"", VA.timestamp)(
        Time.Timestamp.assertFromLong(-3600000000L),
        "\"1970-01-01T00:00:00+01:00\"",
      ),
      cn("\"42\"", "42", VA.int64)(42, "\"+42\""),
      cn("\"0\"", "0", VA.int64)(0, "-0", "\"+0\"", "\"-0\""),
      c("\"Alice\"", VA.party)(Ref.Party assertFromString "Alice"),
      c("{}", VA.unit)(()),
      c("\"2019-06-18\"", VA.date)(Time.Date assertFromString "2019-06-18"),
      c("\"9999-12-31\"", VA.date)(Time.Date assertFromString "9999-12-31"),
      c("\"0001-01-01\"", VA.date)(Time.Date assertFromString "0001-01-01"),
      c("\"abc\"", VA.text)("abc"),
      c("true", VA.bool)(true),
      cn("""["1", "2", "3"]""", "[1, 2, 3]", VA.list(VA.int64))(Vector(1, 2, 3)),
      c("""{"a": "b", "c": "d"}""", VA.map(VA.text))(SortedLookupList(Map("a" -> "b", "c" -> "d"))),
      c("""[["a", "b"], ["c", "d"]]""", VA.genMap(VA.text, VA.text))(Map("a" -> "b", "c" -> "d")),
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
        C.simpleRecordV
      ),
      c("""{"tag": "fA", "value": "foo"}""", C.simpleVariantT)(C.simpleVariantV),
      c("\"Green\"", C.colorGT)(
        C.colorGT get Ref.Name.assertFromString("Green") getOrElse sys.error("impossible")
      ),
    )

    val failures = Table(
      ("JSON", "type", "errorSubstring"),
      ("42.3", VA.int64, ""),
      ("\"42.3\"", VA.int64, ""),
      ("9223372036854775808", VA.int64, ""),
      ("-9223372036854775809", VA.int64, ""),
      ("\"garbage\"", VA.int64, ""),
      ("\"   42 \"", VA.int64, ""),
      ("\"1970-01-01T00:00:00\"", VA.timestamp, ""),
      ("\"1970-01-01T00:00:00+01:00[Europe/Paris]\"", VA.timestamp, ""),
      ("\"0000-01-01\"", VA.date, "Invalid date: 0000-01-01"),
      ("\"9999-99-99\"", VA.date, "Invalid date: 9999-99-99"),
      ("\"9999-12-32\"", VA.date, "Invalid date: 9999-12-32"),
      ("\"9999-13-31\"", VA.date, "Invalid date: 9999-13-31"),
      ("\"10000-01-01\"", VA.date, "Invalid date: 10000-01-01"),
      ("\"1-01-01\"", VA.date, "Invalid date: 1-01-01"),
      ("\"0001-02-29\"", VA.date, "Invalid date: 0001-02-29"),
      ("\"not-a-date\"", VA.date, "Invalid date: not-a-date"),
      ("""{"a": "b", "c": "d"}""", VA.genMap(VA.text, VA.text), ""),
      ("\"\"", VA.party, "Daml-LF Party is empty"),
      (List.fill(256)('a').mkString("\"", "", "\""), VA.party, "Daml-LF Party is too long"),
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
          val tAlternates = Table("alternate", alternates*)
          forEvery(tAlternates) { alternate =>
            val aJson = alternate.parseJson
            typ.prj(jsValueToApiValue(aJson, typ.t, typeLookup)) should ===(Some(expected))
          }
      }

      "fail in cases" in forEvery(failures) { (serialized, typ, errorSubstring) =>
        val json = serialized.parseJson // we don't test *the JSON decoder*
        val exception = the[DeserializationException] thrownBy {
          jsValueToApiValue(json, typ.t, typeLookup)
        }
        exception.getMessage should include(errorSubstring)
      }
    }

    import com.digitalasset.daml.lf.value.Value as LfValue
    import ApiCodecCompressed.JsonImplicits.*

    val packageId: Ref.PackageId = mustBeOne(
      MetadataReader.typeByName(darMetadata)(
        Ref.QualifiedName.assertFromString("JsonEncodingTest:Foo")
      )
    )._1

    val bazRecord = LfValue.ValueRecord(
      None,
      ImmArray(Some(Ref.Name.assertFromString("baz")) -> LfValue.ValueText("text abc")),
    )

    val bazVariant = LfValue.ValueVariant(
      None,
      Ref.Name.assertFromString("Baz"),
      bazRecord,
    )

    val quxVariant = LfValue.ValueVariant(
      None,
      Ref.Name.assertFromString("Qux"),
      LfValue.ValueUnit,
    )

    val fooId =
      Ref.Identifier(packageId, Ref.QualifiedName.assertFromString("JsonEncodingTest:Foo"))

    val bazRecordId =
      Ref.Identifier(packageId, Ref.QualifiedName.assertFromString("JsonEncodingTest:BazRecord"))

    "dealing with LF Record" should {
      val lfType = (n: String) =>
        Ref.Identifier(packageId, Ref.QualifiedName.assertFromString("JsonEncodingTest:" + n))
      val decode = (typeId: Ref.Identifier, json: String) =>
        jsValueToApiValue(json.parseJson, typeId, darTypeLookup)
      val person = (name: String, age: Long, address: String) => {
        val attr = (n: String) => Some(Ref.Name.assertFromString(n))
        LfValue.ValueRecord(
          Some(lfType("Person")),
          ImmArray(
            (attr("name"), LfValue.ValueText(name)),
            (attr("age"), LfValue.ValueInt64(age)),
            (attr("address"), LfValue.ValueText(address)),
          ),
        )
      }
      "decode a JSON array of the right length" in {
        decode(lfType("Person"), """["Joe Smith", 20, "1st Street"]""")
          .shouldBe(person("Joe Smith", 20, "1st Street"))
      }
      "fail to decode if missing fields" in {
        the[DeserializationException].thrownBy {
          decode(lfType("Person"), """["Joe Smith", 21]""")
        }.getMessage should include("expected 3, found 2")
      }
      "fail to decode if extra fields" in {
        the[DeserializationException].thrownBy {
          decode(lfType("Person"), """["Joe Smith", 21, "1st Street", "Arizona"]""")
        }.getMessage should include("expected 3, found 4")
      }
    }

    "dealing with LF Variant" should {
      "encode Foo/Baz to JSON" in {
        val writer = implicitly[spray.json.JsonWriter[LfValue]]
        (writer.write(
          bazVariant
        ): JsValue) shouldBe ("""{"tag":"Baz", "value":{"baz":"text abc"}}""".parseJson: JsValue)
      }

      "decode Foo/Baz from JSON" in {
        val actualValue: LfValue = jsValueToApiValue(
          """{"tag":"Baz", "value":{"baz":"text abc"}}""".parseJson,
          fooId,
          darTypeLookup,
        )

        val expectedValueWithIds: LfValue.ValueVariant =
          bazVariant.copy(tycon = Some(fooId), value = bazRecord.copy(tycon = Some(bazRecordId)))

        actualValue shouldBe expectedValueWithIds
      }

      "encode Foo/Qux to JSON" in {
        val writer = implicitly[spray.json.JsonWriter[LfValue]]
        (writer.write(
          quxVariant
        ): JsValue) shouldBe ("""{"tag":"Qux", "value":{}}""".parseJson: JsValue)
      }

      "fail decoding Foo/Qux from JSON if 'value' field is missing" in {
        assertThrows[spray.json.DeserializationException] {
          jsValueToApiValue(
            """{"tag":"Qux"}""".parseJson,
            fooId,
            darTypeLookup,
          )
        }
      }

      "decode Foo/Qux (empty value) from JSON" in {
        val actualValue: LfValue = jsValueToApiValue(
          """{"tag":"Qux", "value":{}}""".parseJson,
          fooId,
          darTypeLookup,
        )

        val expectedValueWithIds: LfValue.ValueVariant =
          quxVariant.copy(tycon = Some(fooId))

        actualValue shouldBe expectedValueWithIds
      }
    }
  }
}

class ApiCodecCompressedSpecDev extends ApiCodecCompressedSpec {
  override def darPath: String = "JsonEncodingTestDev.dar"

  import com.digitalasset.daml.lf.value.Value as LfValue

  "API compressed JSON codec" when {
    "dealing with Contract Key" should {
      import com.digitalasset.daml.lf.typesig.PackageSignature.TypeDecl.Template as TDTemplate

      "decode type Key = Party from JSON" in {
        val templateDef: TDTemplate = mustBeOne(
          MetadataReader.templateByName(darMetadata)(
            Ref.QualifiedName.assertFromString("JsonEncodingTest:KeyedByParty")
          )
        )._2

        val keyType = templateDef.template.key.getOrElse(fail("Expected a key, got None"))
        val expectedValue: LfValue = LfValue.ValueParty(Ref.Party.assertFromString("Alice"))

        jsValueToApiValue(JsString("Alice"), keyType, darTypeLookup) shouldBe expectedValue
      }

      "decode type Key = (Party, Int) from JSON" in {
        val templateDef: TDTemplate = mustBeOne(
          MetadataReader.templateByName(darMetadata)(
            Ref.QualifiedName.assertFromString("JsonEncodingTest:KeyedByPartyInt")
          )
        )._2

        val tuple2Name = Ref.QualifiedName.assertFromString("DA.Types:Tuple2")
        val daTypesPackageId: Ref.PackageId =
          mustBeOne(MetadataReader.typeByName(darMetadata)(tuple2Name))._1

        val keyType = templateDef.template.key.getOrElse(fail("Expected a key, got None"))

        val expectedValue: LfValue = LfValue.ValueRecord(
          Some(Ref.Identifier(daTypesPackageId, tuple2Name)),
          ImmArray(
            Some(Ref.Name.assertFromString("_1")) -> LfValue.ValueParty(
              Ref.Party.assertFromString("Alice")
            ),
            Some(Ref.Name.assertFromString("_2")) -> LfValue.ValueInt64(123),
          ),
        )

        jsValueToApiValue(
          """["Alice", 123]""".parseJson,
          keyType,
          darTypeLookup,
        ) shouldBe expectedValue
      }

      "decode type Key = (Party, (Int, Foo, BazRecord)) from JSON" in {
        val templateDef: TDTemplate = mustBeOne(
          MetadataReader.templateByName(darMetadata)(
            Ref.QualifiedName.assertFromString("JsonEncodingTest:KeyedByVariantAndRecord")
          )
        )._2

        val keyType = templateDef.template.key.getOrElse(fail("Expected a key, got None"))

        val actual: LfValue = jsValueToApiValue(
          """["Alice", [11, {"tag": "Bar", "value": 123}, {"baz": "baz text"}]]""".parseJson,
          keyType,
          darTypeLookup,
        )

        inside(actual) { case LfValue.ValueRecord(Some(id2), ImmArray((_, party), (_, record2))) =>
          id2.qualifiedName.name shouldBe Ref.DottedName.assertFromString("Tuple2")
          party shouldBe LfValue.ValueParty(Ref.Party.assertFromString("Alice"))

          inside(record2) { case LfValue.ValueRecord(Some(id3), ImmArray((_, age), _, _)) =>
            id3.qualifiedName.name shouldBe Ref.DottedName.assertFromString("Tuple3")
            age shouldBe LfValue.ValueInt64(11)
          }
        }
      }
    }
  }
}
