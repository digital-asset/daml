// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.interpretation.Error.Upgrade.TranslationFailed
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.language._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ArraySeq
import scala.util.{Failure, Success, Try}

class ValueTranslatorSpec_ForbidTrailingNones_V2_1
    extends ValueTranslatorSpec(LanguageVersion.v2_1, forbidTrailingNones = true)
class ValueTranslatorSpec_ForbidTrailingNones_V2_2
    extends ValueTranslatorSpec(LanguageVersion.v2_2, forbidTrailingNones = true)
class ValueTranslatorSpec_AllowTrailingNones_V2_1
    extends ValueTranslatorSpec(LanguageVersion.v2_1, forbidTrailingNones = false)
class ValueTranslatorSpec_AllowTrailingNones_V2_2
    extends ValueTranslatorSpec(LanguageVersion.v2_2, forbidTrailingNones = false)

class ValueTranslatorSpec(languageVersion: LanguageVersion, forbidTrailingNones: Boolean)
    extends AnyWordSpec
    with Inside
    with Matchers
    with TableDrivenPropertyChecks {

  import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
  import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.{
    defaultPackageId => _,
    _,
  }

  val aInt = ValueInt64(42)
  val aText = ValueText("42")
  val aParty = ValueParty("42")
  val someText = ValueOptional(Some(aText))
  val someParty = ValueOptional(Some(aParty))
  val none = Value.ValueNone

  private[this] implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
    ParserParameters.default

  private[this] implicit val defaultPackageId: Ref.PackageId =
    parserParameters.defaultPackageId

  private[this] val aCid =
    ContractId.V1.assertBuild(
      crypto.Hash.hashPrivateKey("a Contract ID"),
      Bytes.assertFromString("00"),
    )

  private[this] val pkg =
    p"""metadata ( 'pkg' : '1.0.0' )

        module Mod {

          record @serializable Tuple (a: *) (b: *) = { x: a, y: b };
          record @serializable Record = { field : Int64 };
          variant @serializable Either (a: *) (b: *) = Left : a | Right : b;
          enum @serializable Color = red | green | blue;

          record @serializable MyCons = { head : Int64, tail: Mod:MyList };
          variant @serializable  MyList = MyNil : Unit | MyCons: Mod:MyCons ;

          record @serializable Template = { field : Int64 };
          record @serializable TemplateRef = { owner: Party, cid: (ContractId Mod:Template) };

          record @serializable Upgradeable = { field: Int64, extraField: (Option Text), anotherExtraField: (Option Text) };
        }
    """

  val upgradablePkgId = Ref.PackageId.assertFromString("-upgradable-v1-")
  val upgradablePkg = {
    implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
      ParserParameters(upgradablePkgId, languageVersion)
    p"""metadata ( 'upgradable' : '1.0.0' )
      module Mod {
        record @serializable Record (a: *) (b: *) (c: *) (d: *) = { fieldA: a, fieldB: Option b, fieldC: Option c };
        variant @serializable Variant (a: *) (b: *) (c: *) = ConsA: a | ConsB: b ;
        enum @serializable Enum = Cons1 | Cons2 ;
        record MyCons = { head : Int64, tail: Mod:MyList };
        variant MyList = MyNil : Unit | MyCons: Mod:MyCons ;
      }
    """
  }

  private[this] val compiledPackage = PureCompiledPackages
    .assertBuild(
      Map(
        defaultPackageId -> pkg,
        upgradablePkgId -> upgradablePkg,
      ),
      Compiler.Config.Default,
    )

  "translateValue" should {

    val valueTranslator = new ValueTranslator(
      compiledPackage.pkgInterface,
      forbidLocalContractIds = false,
      forbidTrailingNones = forbidTrailingNones,
    )
    import valueTranslator.unsafeTranslateValue

    val testCases = Table[Ast.Type, Value, speedy.SValue](
      ("type", "value", "svalue"),
      (TUnit, ValueUnit, SValue.Unit),
      (TBool, ValueTrue, SValue.True),
      (TInt64, ValueInt64(42), SInt64(42)),
      (
        TTimestamp,
        ValueTimestamp(Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")),
        STimestamp(Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")),
      ),
      (
        TDate,
        ValueDate(Time.Date.assertFromString("1879-03-14")),
        SDate(Time.Date.assertFromString("1879-03-14")),
      ),
      (TText, ValueText("daml"), SText("daml")),
      (
        TNumeric(Ast.TNat(Numeric.Scale.assertFromInt(10))),
        ValueNumeric(Numeric.assertFromString("10.0000000000")),
        SNumeric(Numeric.assertFromString("10.0000000000")),
      ),
      (TParty, ValueParty("Alice"), SParty("Alice")),
      (
        TContractId(t"Mod:Template"),
        ValueContractId(aCid),
        SContractId(aCid),
      ),
      (
        TList(TText),
        ValueList(FrontStack(ValueText("a"), ValueText("b"))),
        SList(FrontStack(SText("a"), SText("b"))),
      ),
      (
        TTextMap(TBool),
        ValueTextMap(SortedLookupList(Map("0" -> ValueTrue, "1" -> ValueFalse))),
        SMap(true, SText("0") -> SValue.True, SText("1") -> SValue.False),
      ),
      (
        TGenMap(TInt64, TText),
        ValueGenMap(ImmArray(ValueInt64(1) -> ValueText("1"), ValueInt64(42) -> ValueText("42"))),
        SMap(false, SInt64(1) -> SText("1"), SInt64(42) -> SText("42")),
      ),
      (TOptional(TText), ValueOptional(Some(ValueText("text"))), SOptional(Some(SText("text")))),
      (
        t"Mod:Tuple Int64 Text",
        ValueRecord("", ImmArray("" -> ValueInt64(33), "" -> ValueText("a"))),
        SRecord("Mod:Tuple", ImmArray("x", "y"), ArraySeq(SInt64(33), SText("a"))),
      ),
      (
        t"Mod:Either Int64 Text",
        ValueVariant("", "Right", ValueText("some test")),
        SVariant("Mod:Either", "Right", 1, SText("some test")),
      ),
      (Ast.TTyCon("Mod:Color"), ValueEnum("", "blue"), SEnum("Mod:Color", "blue", 2)),
    )

    val emptyTestCase = Table[Ast.Type, Value, speedy.SValue](
      ("type", "value", "svalue"),
      (TList(TText), ValueList(FrontStack.empty), SList(FrontStack.empty)),
      (
        TOptional(TText),
        ValueOptional(None),
        SOptional(None),
      ),
      (
        TTextMap(TText),
        ValueTextMap(SortedLookupList.Empty),
        SMap(true),
      ),
      (
        TGenMap(TInt64, TText),
        ValueGenMap(ImmArray.empty),
        SMap(false),
      ),
    )

    "succeeds on well typed values" in {
      forAll(testCases ++ emptyTestCase) { (typ, value, svalue) =>
        Try(unsafeTranslateValue(typ, value)) shouldBe Success(svalue)
      }
    }

    "fail on non-strictly ordered maps" in {

      val cases = Table[Ast.Type, Value](
        ("type", "value"),
        (
          TGenMap(TInt64, TText),
          ValueGenMap(ImmArray(ValueInt64(1) -> ValueText("1"), ValueInt64(0) -> ValueText("0"))),
        ),
        (
          TGenMap(TInt64, TText),
          ValueGenMap(ImmArray(ValueInt64(0) -> ValueText("0"), ValueInt64(0) -> ValueText("0"))),
        ),
      )

      forAll(cases) { (typ, value) =>
        inside(Try(unsafeTranslateValue(typ, value))) {
          case Failure(error: TranslationFailed.Error) =>
            error shouldBe a[TranslationFailed.InvalidValue]
        }
      }
    }

    "return proper mismatch error for upgrades" in {

      implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
        ParserParameters(upgradablePkgId, languageVersion)

      val TRecordUpgradable = t"Mod:Record Int64 Text Party Unit"

      val TVariantUpgradable = t"Mod:Variant Int64 Text Unit"

      val TEnumUpgradable = t"Mod:Enum"

      val testCases = Table[Ast.Type, Value, PartialFunction[TranslationFailed.Error, _]](
        ("type", "value", "error"),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray(
              "" -> aInt,
              "" -> someParty, // Here the field has type Party instead of Text
            ),
          ),
          { case TranslationFailed.TypeMismatch(typ, value, _) =>
            typ shouldBe t"Text"
            value shouldBe aParty
          },
        ),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray(), // missing a non-optional field
          ),
          { case TranslationFailed.TypeMismatch(typ, _, _) =>
            typ shouldBe TRecordUpgradable
          },
        ),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray(
              "" -> aInt,
              "" -> someText,
              "" -> someParty,
              "" -> aInt, // extra non-optional field
            ),
          ),
          { case TranslationFailed.TypeMismatch(typ, _, _) =>
            typ shouldBe TRecordUpgradable
          },
        ),
        (
          TVariantUpgradable,
          ValueVariant("", "ConsB", aInt), // Here the variant has type Text instead of Int64
          { case TranslationFailed.TypeMismatch(typ, value, _) =>
            typ shouldBe t"Text"
            value shouldBe aInt
          },
        ),
        (
          TNumeric(Ast.TNat(Numeric.Scale.assertFromInt(10))),
          ValueNumeric(Numeric.assertFromString("10.000")), // scale != 10
          { case TranslationFailed.TypeMismatch(typ, value, _) =>
            typ shouldBe t"Numeric 10"
            value shouldBe ValueNumeric(Numeric.assertFromString("10.000"))
          },
        ),
        (
          TVariantUpgradable,
          ValueVariant("", "ConsC", aInt), // ConsC is not a constructor of Mod:Variant
          {
            case TranslationFailed.LookupError(
                  LookupError.NotFound(
                    Reference.DataVariantConstructor(_, consName),
                    Reference.DataVariantConstructor(_, _),
                  )
                ) =>
              consName shouldBe "ConsC"
          },
        ),
        (
          TEnumUpgradable,
          ValueEnum("", "Cons3"), // Cons3 is not a constructor of Mod:Enum
          {
            case TranslationFailed.LookupError(
                  LookupError.NotFound(
                    Reference.DataEnumConstructor(_, consName),
                    Reference.DataEnumConstructor(_, _),
                  )
                ) =>
              consName shouldBe "Cons3"
          },
        ),
        (
          TVariantUpgradable,
          ValueVariant("", "ConsC", aInt), // ConsC is not a constructor of Mod:Variant
          {
            case TranslationFailed.LookupError(
                  LookupError.NotFound(
                    Reference.DataVariantConstructor(_, consName),
                    Reference.DataVariantConstructor(_, _),
                  )
                ) =>
              consName shouldBe "ConsC"
          },
        ),
        (
          TEnumUpgradable,
          ValueEnum("", "Cons3"), // Cons3 is not a constructor of Mod:Enum
          {
            case TranslationFailed.LookupError(
                  LookupError.NotFound(
                    Reference.DataEnumConstructor(_, consName),
                    Reference.DataEnumConstructor(_, _),
                  )
                ) =>
              consName shouldBe "Cons3"
          },
        ),
      )
      forEvery(testCases)((typ, value, checkError) =>
        inside(Try(unsafeTranslateValue(typ, value))) {
          case Failure(error: TranslationFailed.Error) =>
            checkError(error)
        }
      )
    }

    "handle different representation of the same upgraded/downgraded record" in {
      val typ = t"Mod:Upgradeable"

      def sValue(extraFieldDefined: Boolean, anotherExtraFieldDefined: Boolean) =
        SRecord(
          "Mod:Upgradeable",
          ImmArray("field", "extraField", "anotherExtraField"),
          ArraySeq(
            SInt64(1),
            SOptional(Some(SText("a")).filter(Function.const(extraFieldDefined))),
            SOptional(Some(SText("b")).filter(Function.const(anotherExtraFieldDefined))),
          ),
        )

      def upgradeCaseSuccess(
          extraFieldDefined: Boolean,
          anotherExtraFieldDefined: Boolean,
          value: Value,
      ) =
        (Success(sValue(extraFieldDefined, anotherExtraFieldDefined)), value)

      def upgradeCaseFailure(s: String, value: Value) =
        (Failure(TranslationFailed.TypeMismatch(typ, value, s)), value)

      val testCases = Table(
        ("", "record"),
        upgradeCaseSuccess(
          true,
          true,
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1),
              "" -> ValueOptional(Some(ValueText("a"))),
              "" -> ValueOptional(Some(ValueText("b"))),
            ),
          ),
        ),
        upgradeCaseSuccess(
          false,
          true,
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1),
              "" -> ValueOptional(None),
              "" -> ValueOptional(Some(ValueText("b"))),
            ),
          ),
        ),
        upgradeCaseSuccess(
          false,
          false,
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1)
            ),
          ),
        ),
        upgradeCaseFailure(
          "Found non-optional extra field at index 3, cannot remove for downgrading.",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1),
              "" -> ValueOptional(None),
              "" -> ValueOptional(None),
              "" -> ValueText("bad"),
            ),
          ),
        ),
        upgradeCaseFailure(
          "Unexpected non-optional extra template field type encountered during upgrading.",
          ValueRecord("", ImmArray()),
        ),
      )

      forEvery(testCases)((result, value) => Try(unsafeTranslateValue(typ, value)) shouldBe result)
    }

    if (!forbidTrailingNones) {
      "handle different representation of the same upgraded/downgraded record with trailing nones" in {
        val typ = t"Mod:Upgradeable"
        val sValue =
          SRecord(
            "Mod:Upgradeable",
            ImmArray("field", "extraField", "anotherExtraField"),
            ArraySeq(
              SInt64(1),
              SOptional(None),
              SOptional(None),
            ),
          )

        val testCases = Table[ValueRecord](
          "record",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1),
              "" -> ValueOptional(None),
            ),
          ),
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1),
              "" -> ValueOptional(None),
              "" -> ValueOptional(None),
            ),
          ),
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1),
              "" -> ValueOptional(None),
              "" -> ValueOptional(None),
              "" -> ValueOptional(None),
            ),
          ),
        )
        forEvery(testCases)(value => Try(unsafeTranslateValue(typ, value)) shouldBe Success(sValue))
      }
    }

    "handle different representation of the same variant" in {
      val typ = t"Mod:Either Text Int64"
      val testCases = Table(
        "variant",
        ValueVariant("", "Left", ValueText("some test")),
      )
      val svalue = SVariant("Mod:Either", "Left", 0, SText("some test"))

      forEvery(testCases)(value => Try(unsafeTranslateValue(typ, value)) shouldBe Success(svalue))
    }

    "handle different representation of the same enum" in {
      val typ = t"Mod:Color"
      val testCases = Table("enum", ValueEnum("", "green"))
      val svalue = SEnum("Mod:Color", "green", 1)
      forEvery(testCases)(value => Try(unsafeTranslateValue(typ, value)) shouldBe Success(svalue))
    }

    "return proper mismatch error" in {
      val res = Try(
        unsafeTranslateValue(
          t"Mod:Tuple Int64 Text",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(33),
              "" -> ValueParty("Alice"), // Here the field has type Party instead of Text
            ),
          ),
        )
      )
      inside(res) { case Failure(TranslationFailed.TypeMismatch(typ, value, _)) =>
        typ shouldBe t"Text"
        value shouldBe ValueParty("Alice")
      }
    }

    "fails on non-well typed values" in {
      forAll(testCases) { (typ1, value1, _) =>
        forAll(testCases) { (_, value2, _) =>
          if (value1 != value2) {
            a[TranslationFailed.Error] shouldBe thrownBy(
              unsafeTranslateValue(typ1, value2)
            )
          }
        }
      }
    }

    "fails on too deep values" in {

      def mkMyList(n: Int) =
        Iterator.range(0, n).foldLeft[Value](ValueVariant("", "MyNil", ValueUnit)) { case (v, n) =>
          ValueVariant(
            "",
            "MyCons",
            ValueRecord("", ImmArray("" -> ValueInt64(n.toLong), "" -> v)),
          )
        }

      val notTooBig = mkMyList(49)
      val tooBig = mkMyList(50)
      val failure = Failure(TranslationFailed.ValueNesting)

      Try(unsafeTranslateValue(t"Mod:MyList", notTooBig)) shouldBe a[Success[_]]
      Try(unsafeTranslateValue(t"Mod:MyList", tooBig)) shouldBe failure
    }

    "fails on values containing null characters" in {

      val testCases = Table(
        ("type", "value without null char", "value with null char"),
        (TText, Value.ValueText("->\u0001<-"), Value.ValueText("->\u0000<-")),
        (
          TOptional(TText),
          ValueOptional(Some(ValueText("'\u0001'+'\u0001'='\u0002'"))),
          ValueOptional(Some(ValueText("'\u0001'-'\u0001'='\u0000'"))),
        ),
        (
          TGenMap(TText, TInt64),
          ValueGenMap(
            ImmArray(
              ValueText("\u0001") -> ValueInt64(1)
            )
          ),
          ValueGenMap(
            ImmArray(
              ValueText("\u0000") -> ValueInt64(0),
              ValueText("\u0001") -> ValueInt64(1),
            )
          ),
        ),
        (
          TTextMap(TInt64),
          ValueTextMap(
            SortedLookupList(
              Map(
                "\u0001" -> ValueInt64(1)
              )
            )
          ),
          ValueTextMap(
            SortedLookupList(
              Map(
                "\u0000" -> ValueInt64(0),
                "\u0001" -> ValueInt64(2),
              )
            )
          ),
        ),
      )

      forEvery(testCases) { case (typ, negativeTestCase, positiveTestCase) =>
        val success = Try(unsafeTranslateValue(typ, negativeTestCase))
        val failure = Try(unsafeTranslateValue(typ, positiveTestCase))
        success shouldBe a[Success[_]]
        inside(failure) { case Failure(TranslationFailed.MalformedText(err)) =>
          err should include("null character")
        }
      }
    }

    def testCasesForCid(culprit: ContractId) = {
      val cid = ValueContractId(culprit)
      Table[Ast.Type, Value](
        ("type" -> "value"),
        t"ContractId Mod:Template" -> cid,
        TList(t"ContractId Mod:Template") -> ValueList(FrontStack(cid)),
        TTextMap(t"ContractId Mod:Template") -> ValueTextMap(SortedLookupList(Map("0" -> cid))),
        TGenMap(TInt64, t"ContractId Mod:Template") -> ValueGenMap(ImmArray(ValueInt64(1) -> cid)),
        TGenMap(t"ContractId Mod:Template", TInt64) -> ValueGenMap(ImmArray(cid -> ValueInt64(0))),
        TOptional(t"ContractId Mod:Template") -> ValueOptional(Some(cid)),
        t"Mod:TemplateRef" -> ValueRecord(
          "",
          ImmArray("" -> ValueParty("Alice"), "" -> cid),
        ),
        TTyConApp("Mod:Either", ImmArray(t"ContractId Mod:Template", TInt64)) -> ValueVariant(
          "",
          "Left",
          cid,
        ),
      )
    }

    "accept all contract IDs when require flags are false" in {

      val valueTranslator = new ValueTranslator(
        compiledPackage.pkgInterface,
        forbidLocalContractIds = false,
        forbidTrailingNones = forbidTrailingNones,
      )
      val unsuffixedCidV1 = ContractId.V1
        .assertBuild(crypto.Hash.hashPrivateKey("a non-suffixed V1 Contract ID"), Bytes.Empty)
      val suffixedCidV1 = ContractId.V1.assertBuild(
        crypto.Hash.hashPrivateKey("a suffixed V1 Contract ID"),
        Bytes.assertFromString("00"),
      )
      val unsuffixedCidV2 = ContractId.V2.unsuffixed(
        Time.Timestamp.Epoch,
        crypto.Hash.hashPrivateKey("an unsuffixed V2 Contract ID"),
      )
      val suffixedCidV2 =
        ContractId.V2.assertBuild(unsuffixedCidV2.local, Bytes.assertFromString("00"))
      val cids = List(suffixedCidV1, unsuffixedCidV1, suffixedCidV2, unsuffixedCidV2)

      cids.foreach(cid =>
        forEvery(testCasesForCid(cid))((typ, value) =>
          Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe a[Success[_]]
        )
      )
    }

    "reject non suffixed V1/V2 Contract IDs when forbidLocalContractIds is true" in {

      val valueTranslator = new ValueTranslator(
        compiledPackage.pkgInterface,
        forbidLocalContractIds = true,
        forbidTrailingNones = forbidTrailingNones,
      )
      val legalCidV1 =
        ContractId.V1.assertBuild(
          crypto.Hash.hashPrivateKey("a legal Contract ID"),
          Bytes.assertFromString("00"),
        )
      val legalCidV2 = ContractId.V2.assertBuild(
        Bytes.fromByteArray(Array.fill[Byte](ContractId.V2.localSize)(0x12.toByte)),
        Bytes.assertFromString("00"),
      )
      val illegalCidV1 =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("an illegal Contract ID"), Bytes.Empty)
      val illegalCidV2 = ContractId.V2.unsuffixed(
        Time.Timestamp.Epoch,
        crypto.Hash.hashPrivateKey("an illegal Contract ID"),
      )
      val failureV1 =
        Failure(TranslationFailed.NonSuffixedV1ContractId(illegalCidV1))
      val failureV2 =
        Failure(TranslationFailed.NonSuffixedV2ContractId(illegalCidV2))

      forEvery(testCasesForCid(legalCidV1))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe a[Success[_]]
      )
      forEvery(testCasesForCid(legalCidV2))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe a[Success[_]]
      )
      forEvery(testCasesForCid(illegalCidV1))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe failureV1
      )
      forEvery(testCasesForCid(illegalCidV2))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe failureV2
      )
    }

    "reject records with typeCon IDs" in {
      implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
        ParserParameters(upgradablePkgId, languageVersion)

      val testCases = Table[Ast.Type, Value](
        ("type", "value"),
        (
          t"Mod:Record Int64 Text Party Unit",
          ValueRecord(
            "Mod:Upgradeable",
            ImmArray(
              "" -> ValueInt64(1),
              "" -> ValueOptional(None),
              "" -> ValueOptional(None),
            ),
          ),
        ),
        (
          t"Mod:Record (Mod:Record Int64 Text Party Unit) Text Party Unit",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueRecord[Nothing](
                "Mod:Upgradeable", // outer record is fine but the nested record has a typeCon ID
                ImmArray(
                  "" -> ValueInt64(1),
                  "" -> ValueOptional(None),
                  "" -> ValueOptional(None),
                ),
              ),
              "" -> ValueOptional(None),
              "" -> ValueOptional(None),
            ),
          ),
        ),
      )
      forAll(testCases) { (typ, value) =>
        a[TranslationFailed.InvalidValue] shouldBe thrownBy(
          unsafeTranslateValue(typ, value)
        )
      }
    }

    "reject variants with typeCon IDs" in {
      implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
        ParserParameters(upgradablePkgId, languageVersion)

      val testCases = Table[Ast.Type, Value](
        ("type", "value"),
        (
          t"Mod:Variant Unit Unit Unit",
          ValueVariant(
            "Mod:Variant",
            "ConsA",
            ValueUnit,
          ),
        ),
        (
          t"Mod:Record (Mod:Variant Unit Unit Unit) Text Party Unit",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueVariant(
                "Mod:Variant",
                "ConsA",
                ValueUnit,
              ),
              "" -> ValueOptional(None),
              "" -> ValueOptional(None),
            ),
          ),
        ),
      )
      forAll(testCases) { (typ, value) =>
        a[TranslationFailed.InvalidValue] shouldBe thrownBy(
          unsafeTranslateValue(typ, value)
        )
      }
    }

    "reject enums with typeCon IDs" in {
      implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
        ParserParameters(upgradablePkgId, languageVersion)

      val testCases = Table[Ast.Type, Value](
        ("type", "value"),
        (
          t"Mod:Enum",
          ValueEnum(
            "Mod:Enum",
            "red",
          ),
        ),
        (
          t"Mod:Record Mod:Enum Text Party Unit",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueEnum(
                "Mod:Enum",
                "red",
              ),
              "" -> ValueOptional(None),
              "" -> ValueOptional(None),
            ),
          ),
        ),
      )
      forAll(testCases) { (typ, value) =>
        a[TranslationFailed.InvalidValue] shouldBe thrownBy(
          unsafeTranslateValue(typ, value)
        )
      }
    }

    "reject records with labels" in {
      implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
        ParserParameters(upgradablePkgId, languageVersion)

      val testCases = Table[Ast.Type, Value](
        ("type", "value"),
        (
          t"Mod:Record Int64 Text Party Unit",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1),
              "fieldB" -> ValueOptional(None),
              "" -> ValueOptional(None),
            ),
          ),
        ),
        (
          t"Mod:Record (Mod:Record Int64 Text Party Unit) Text Party Unit",
          ValueRecord[Nothing](
            "",
            ImmArray(
              "" -> ValueRecord[Nothing](
                "",
                ImmArray(
                  "" -> ValueInt64(1),
                  "fieldB" -> ValueOptional(
                    None
                  ), // outer record is fine but the nested record has a label
                  "" -> ValueOptional(None),
                ),
              ),
              "" -> ValueOptional(None),
              "" -> ValueOptional(None),
            ),
          ),
        ),
      )
      forAll(testCases) { (typ, value) =>
        a[TranslationFailed.InvalidValue] shouldBe thrownBy(
          unsafeTranslateValue(typ, value)
        )
      }
    }

    val trailingNonesTestCases = {
      implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
        ParserParameters(upgradablePkgId, LanguageVersion.v2_1)

      Table[Ast.Type, Value](
        ("type", "value"),
        (
          t"Mod:Record Int64 Text Int64 Unit",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1),
              "" -> ValueOptional(Some(ValueText("a"))),
              "" -> ValueOptional(None),
            ),
          ),
        ),
        (
          t"Mod:Record Int64 Text Int64 Unit",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1),
              "" -> ValueOptional(None),
              "" -> ValueOptional(None),
            ),
          ),
        ),
        (
          t"Mod:Record Int64 Text Int64 Unit",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueInt64(1),
              "" -> ValueOptional(None),
              "" -> ValueOptional(Some(ValueInt64(2))),
              "" -> ValueOptional(None),
            ),
          ),
        ),
        (
          t"Mod:Record (Mod:Record Int64 Text Int64 Unit) Text Int64 Unit",
          ValueRecord(
            "",
            ImmArray(
              "" -> ValueRecord[Nothing](
                "",
                ImmArray(
                  "" -> ValueInt64(1),
                  "" -> ValueOptional(None),
                  "" -> ValueOptional(None),
                  "" -> ValueOptional(None),
                ),
              ),
              "" -> ValueOptional(None),
              "" -> ValueOptional(Some(ValueInt64(2))),
              "" -> ValueOptional(None),
            ),
          ),
        ),
      )
    }

    if (forbidTrailingNones) {
      "reject records with trailing Nones" in {
        forAll(trailingNonesTestCases) { (typ, value) =>
          a[TranslationFailed.InvalidValue] shouldBe thrownBy(
            unsafeTranslateValue(typ, value)
          )
        }
      }
    } else {
      "allow records with trailing Nones" in {
        forAll(trailingNonesTestCases) { (typ, value) =>
          Try(unsafeTranslateValue(typ, value)) shouldBe a[Success[_]]
        }
      }
    }
  }
}
