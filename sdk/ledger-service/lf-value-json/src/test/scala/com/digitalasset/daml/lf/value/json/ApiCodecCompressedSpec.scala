// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value
package json

import com.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.value.Value.{ContractId, ValueList, ValueText}
import data.{FrontStack, ImmArray, Numeric, Ref, SortedLookupList, Time}
import value.test.TypedValueGenerators.{genAddend, genTypeAndValue, ValueAddend => VA}
import value.test.ValueGenerators.coidGen
import ApiCodecCompressed.{apiValueToJsValue, jsValueToApiValue}
import com.digitalasset.daml.lf.archive.DarSchemaDecoder
import com.digitalasset.daml.lf.language.{
  Ast,
  LanguageVersion,
  PackageInterface,
  TypeDestructor,
  Util => AstUtil,
}
import org.scalactic.source
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.Arbitrary
import spray.json._

import scala.annotation.nowarn
import scala.util.{Success, Try}

class ApiCodecCompressedSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with Inside {

  import C.typeDestructor

  private[this] implicit val cidArb: Arbitrary[ContractId] = Arbitrary(coidGen)

  private val dar = new java.io.File(rlocation("ledger-service/lf-value-json/JsonEncodingTest.dar"))
  require(dar.exists())

  private val darDar = DarSchemaDecoder.assertReadArchiveFromFile(dar)
  private val darPackageId: Ref.PackageId = darDar.main._1
  private val darPackageInterface: PackageInterface =
    new PackageInterface((darDar.main :: darDar.dependencies).toMap)
  private val darTypeDestructor: TypeDestructor = TypeDestructor(darPackageInterface)

  private def darPackageIdOf(qn: Ref.QualifiedName): Ref.PackageId =
    (darDar.main :: darDar.dependencies)
      .collectFirst {
        case (pid, sig) if sig.modules.get(qn.module).exists(_.definitions.contains(qn.name)) =>
          pid
      }
      .getOrElse(fail(s"cannot find package defining $qn"))

  /** Serializes the API value to JSON, then parses it back to an API value */
  private def serializeAndParse(
      value: Value,
      typ: Ast.Type,
  ): Try[Value] = {
    import ApiCodecCompressed.JsonImplicits._

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(jsValueToApiValue(json, typ, typeDestructor))
    } yield parsed
  }

  private def roundtrip(va: VA)(v: va.Inj): Option[va.Inj] =
    va.prj(jsValueToApiValue(apiValueToJsValue(va.inj(v)), va.t, typeDestructor))

  private val decimalScale = Numeric.Scale.assertFromInt(10)

  private object C /* based on navigator DamlConstants */ {
    val packageId0 = Ref.PackageId assertFromString "hash"
    val moduleName0 = Ref.ModuleName assertFromString "Module"
    def defRef(name: Ref.DottedName) =
      Ref.Identifier(
        packageId0,
        Ref.QualifiedName(moduleName0, name),
      )

    import Ref.Name.{assertFromString => name}
    def nameOpt(s: String): Option[Ref.Name] = Some(name(s))

    val emptyRecordN = Ref.DottedName.assertFromString("EmptyRecord")
    val emptyRecordId = defRef(emptyRecordN)
    val emptyRecordDDT = Ast.DDataType(
      serializable = true,
      params = ImmArray.empty,
      cons = Ast.DataRecord(ImmArray.empty),
    )
    val emptyRecordT = Ast.TTyCon(emptyRecordId)
    val emptyRecordV = Value.ValueRecord(Some(emptyRecordId), ImmArray.empty)

    val simpleRecordN = Ref.DottedName.assertFromString("SimpleRecord")
    val simpleRecordId = defRef(simpleRecordN)
    val simpleRecordDDT = Ast.DDataType(
      serializable = true,
      params = ImmArray.empty,
      cons = Ast.DataRecord(
        ImmArray(
          name("fA") -> AstUtil.TText,
          name("fB") -> AstUtil.TInt64,
        )
      ),
    )

    val simpleRecordT = Ast.TTyCon(simpleRecordId)
    val simpleRecordV: Value = Value.ValueRecord(
      Some(simpleRecordId),
      ImmArray(
        nameOpt("fA") -> Value.ValueText("foo"),
        nameOpt("fB") -> Value.ValueInt64(100),
      ),
    )

    val simpleVariantN = Ref.DottedName.assertFromString("SimpleVariant")
    val simpleVariantId = defRef(simpleVariantN)
    val simpleVariantDDT = Ast.DDataType(
      serializable = true,
      params = ImmArray.empty,
      cons = Ast.DataVariant(
        ImmArray(
          name("fA") -> AstUtil.TText,
          name("fB") -> AstUtil.TInt64,
        )
      ),
    )

    val simpleVariantT = Ast.TTyCon(simpleVariantId)
    val simpleVariantV =
      Value.ValueVariant(
        Some(simpleVariantId),
        name("fA"),
        Value.ValueText("foo"),
      )

    val complexRecordN = Ref.DottedName.assertFromString("ComplexRecord")
    val complexRecordId = defRef(complexRecordN)
    val complexRecordDDT = Ast.DDataType(
      serializable = true,
      params = ImmArray.empty,
      cons = Ast.DataRecord(
        ImmArray[(Ref.Name, Ast.Type)](
          name("fText") -> AstUtil.TText,
          name("fBool") -> AstUtil.TBool,
          name("fDecimal") -> AstUtil.TDecimal,
          name("fUnit") -> AstUtil.TUnit,
          name("fInt64") -> AstUtil.TInt64,
          name("fParty") -> AstUtil.TParty,
          name("fContractId") -> AstUtil.TContractId(AstUtil.TUnit),
          name("fListOfText") -> AstUtil.TList(AstUtil.TText),
          name("fListOfUnit") -> AstUtil.TList(AstUtil.TUnit),
          name("fDate") -> AstUtil.TDate,
          name("fTimestamp") -> AstUtil.TTimestamp,
          name("fOptionalText") -> AstUtil.TOptional(AstUtil.TText),
          name("fOptionalUnit") -> AstUtil.TOptional(AstUtil.TUnit),
          name("fOptOptText") -> AstUtil.TOptional(AstUtil.TOptional(AstUtil.TText)),
          name("fMap") -> AstUtil.TTextMap(AstUtil.TInt64),
          name("fVariant") -> simpleVariantT,
          name("fRecord") -> simpleRecordT,
        )
      ),
    )
    val complexRecordT = Ast.TTyCon(complexRecordId)
    val complexRecordV =
      Value.ValueRecord(
        Some(complexRecordId),
        ImmArray[(Option[Ref.Name], Value)](
          nameOpt("fText") -> Value.ValueText("foo"),
          nameOpt("fBool") -> Value.ValueBool(true),
          nameOpt("fDecimal") -> Value.ValueNumeric(Numeric.assertFromString("100.0000000000")),
          nameOpt("fUnit") -> Value.ValueUnit,
          nameOpt("fInt64") -> Value.ValueInt64(100),
          nameOpt("fParty") -> Value.ValueParty(Ref.Party.assertFromString("BANK1")),
          nameOpt("fContractId") -> Value.ValueContractId(
            Value.ContractId.assertFromString("00" + "00" * 32 + "c0")
          ),
          nameOpt("fListOfText") -> ValueList(
            FrontStack(Value.ValueText("foo"), Value.ValueText("bar"))
          ),
          nameOpt("fListOfUnit") -> ValueList(FrontStack(Value.ValueUnit, Value.ValueUnit)),
          nameOpt("fDate") -> Value.ValueDate(Time.Date.assertFromString("2019-01-28")),
          nameOpt("fTimestamp") -> Value.ValueTimestamp(
            Time.Timestamp assertFromString "2019-01-28T12:44:33.22Z"
          ),
          nameOpt("fOptionalText") -> Value.ValueOptional(None),
          nameOpt("fOptionalUnit") -> Value.ValueOptional(Some(Value.ValueUnit)),
          nameOpt("fOptOptText") -> Value.ValueOptional(
            Some(Value.ValueOptional(Some(ValueText("foo"))))
          ),
          nameOpt("fMap") -> Value.ValueTextMap(
            SortedLookupList.from(
              Map(
                "1" -> Value.ValueInt64(1L),
                "2" -> Value.ValueInt64(2L),
                "3" -> Value.ValueInt64(3L),
              )
            )
          ),
          nameOpt("fVariant") -> simpleVariantV,
          nameOpt("fRecord") -> simpleRecordV,
        ),
      )

    val colorN = Ref.DottedName.assertFromString("Color")
    val colorId = defRef(colorN)
    val (colorGD, colorGT) =
      VA.enumeration(colorId, Seq("Red", "Green", "Blue") map Ref.Name.assertFromString)

    val typeDestructor = TypeDestructor(
      PackageInterface(
        Map(
          packageId0 -> Ast.Package(
            modules = Map(
              moduleName0 -> Ast.Module(
                name = moduleName0,
                definitions = Map(
                  emptyRecordN -> emptyRecordDDT,
                  simpleRecordN -> simpleRecordDDT,
                  simpleVariantN -> simpleVariantDDT,
                  complexRecordN -> complexRecordDDT,
                  colorN -> colorGD,
                ),
                templates = Map.empty,
                exceptions = Map.empty,
                interfaces = Map.empty,
                featureFlags = Ast.FeatureFlags.default,
              )
            ),
            directDeps = Set.empty,
            languageVersion = LanguageVersion.stableLfVersionsRange.max,
            metadata = Ast.PackageMetadata(
              name = Ref.PackageName.assertFromString("JsonEncodingTest"),
              version = Ref.PackageVersion.assertFromString("1.0.0"),
              upgradedPackageId = None,
            ),
            imports = Ast.DeclaredImports(Set.empty),
          )
        )
      )
    )
  }

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
        forAll(minSuccessful(20)) { v: va.Inj =>
          roundtrip(va)(v) shouldBe Some(v)
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
        forAll(minSuccessful(1000)) { v: va.Inj =>
          roundtrip(va)(v) should ===(Some(v))
        }
      }

      val roundtrips = Table(
        ("type", "Daml value"),
        (C.emptyRecordT, C.emptyRecordV),
        (C.simpleRecordT, C.simpleRecordV),
        (C.simpleVariantT, C.simpleVariantV),
        (C.complexRecordT, C.complexRecordV),
      )
      "work for records and variants" in forEvery(roundtrips) { (typ, damlValue) =>
        jsValueToApiValue(apiValueToJsValue(damlValue), typ, typeDestructor) shouldBe damlValue
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
      cn(canonical, canonical, typ)(expected, alternates: _*)(pos)

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
        Numeric.assertFromString("42.0000000000"),
        "\"42\"",
        "42",
        "42.0",
        "\"+42\"",
      ),
      cn("\"2000.0\"", "2000", VA.numeric(decimalScale))(
        Numeric.assertFromString("2000.0000000000"),
        "\"2000\"",
        "2000",
        "2e3",
      ),
      cn("\"0.3\"", "0.3", VA.numeric(decimalScale))(
        Numeric.assertFromString("0.3000000000"),
        "\"0.30000000000000004\"",
        "0.30000000000000004",
      ),
      cn(
        "\"9999999999999999999999999999.9999999999\"",
        "9999999999999999999999999999.9999999999",
        VA.numeric(decimalScale),
      )(Numeric.assertFromString("9999999999999999999999999999.9999999999")),
      cn("\"0.1234512346\"", "0.1234512346", VA.numeric(decimalScale))(
        Numeric.assertFromString("0.1234512346"),
        "0.12345123455",
        "0.12345123465",
        "\"0.12345123455\"",
        "\"0.12345123465\"",
      ),
      cn("\"0.1234512345\"", "0.1234512345", VA.numeric(decimalScale))(
        Numeric.assertFromString("0.1234512345"),
        "0.123451234549",
        "0.12345123445001",
        "\"0.123451234549\"",
        "\"0.12345123445001\"",
      ),
      c("\"1990-11-09T04:30:23.123456Z\"", VA.timestamp)(
        Time.Timestamp assertFromString "1990-11-09T04:30:23.123456Z",
        "\"1990-11-09T04:30:23.1234569Z\"",
      ),
      c("\"1970-01-01T00:00:00Z\"", VA.timestamp)(Time.Timestamp assertFromLong 0),
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
      c("""{"a": "b", "c": "d"}""", VA.map(VA.text))(
        SortedLookupList.from(Map("a" -> "b", "c" -> "d"))
      ),
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
          val parsed = jsValueToApiValue(json, typ.t, typeDestructor)
          jsValueToApiValue(numJson, typ.t, typeDestructor) should ===(parsed)
          typ.prj(parsed) should ===(Some(expected))
          apiValueToJsValue(parsed) should ===(json)
          numCodec.apiValueToJsValue(parsed) should ===(numJson)
          val tAlternates = Table("alternate", alternates: _*)
          forEvery(tAlternates) { alternate =>
            val aJson = alternate.parseJson
            typ.prj(jsValueToApiValue(aJson, typ.t, typeDestructor)) should ===(Some(expected))
          }
      }

      "fail in cases" in forEvery(failures) { (serialized, typ, errorSubstring) =>
        val json = serialized.parseJson // we don't test *the JSON decoder*
        val exception = the[DeserializationException] thrownBy {
          jsValueToApiValue(json, typ.t, typeDestructor)
        }
        exception.getMessage should include(errorSubstring)
      }
    }

    import com.digitalasset.daml.lf.value.{Value => LfValue}
    import ApiCodecCompressed.JsonImplicits._

    val packageId: Ref.PackageId = darPackageId

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
        jsValueToApiValue(json.parseJson, Ast.TTyCon(typeId), darTypeDestructor)
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
          Ast.TTyCon(fooId),
          darTypeDestructor,
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
            Ast.TTyCon(fooId),
            darTypeDestructor,
          )
        }
      }

      "decode Foo/Qux (empty value) from JSON" in {
        val actualValue: LfValue = jsValueToApiValue(
          """{"tag":"Qux", "value":{}}""".parseJson,
          Ast.TTyCon(fooId),
          darTypeDestructor,
        )

        val expectedValueWithIds: LfValue.ValueVariant =
          quxVariant.copy(tycon = Some(fooId))

        actualValue shouldBe expectedValueWithIds
      }
    }

    "dealing with Contract Key" should {
      def keyType(template: String): Ast.Type =
        darPackageInterface
          .lookupTemplateKey(
            Ref.Identifier(packageId, Ref.QualifiedName.assertFromString(template))
          )
          .map(_.typ)
          .getOrElse(fail("Expected a key, got None"))

      "decode type Key = Party from JSON" in {
        val expectedValue: LfValue = LfValue.ValueParty(Ref.Party.assertFromString("Alice"))

        jsValueToApiValue(
          JsString("Alice"),
          keyType("JsonEncodingTest:KeyedByParty"),
          darTypeDestructor,
        ) shouldBe expectedValue
      }

      "decode type Key = (Party, Int) from JSON" in {
        val tuple2Name = Ref.QualifiedName.assertFromString("DA.Types:Tuple2")
        val daTypesPackageId: Ref.PackageId = darPackageIdOf(tuple2Name)

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
          keyType("JsonEncodingTest:KeyedByPartyInt"),
          darTypeDestructor,
        ) shouldBe expectedValue
      }

      "decode type Key = (Party, (Int, Foo, BazRecord)) from JSON" in {
        val actual: LfValue = jsValueToApiValue(
          """["Alice", [11, {"tag": "Bar", "value": 123}, {"baz": "baz text"}]]""".parseJson,
          keyType("JsonEncodingTest:KeyedByVariantAndRecord"),
          darTypeDestructor,
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
