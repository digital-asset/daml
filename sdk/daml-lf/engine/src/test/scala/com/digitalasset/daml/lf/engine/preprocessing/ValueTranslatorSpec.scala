// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package preprocessing

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.{
  Ast,
  LanguageMajorVersion,
  LanguageVersion,
  LookupError,
  Reference,
}
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.speedy.ArrayList
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import com.digitalasset.daml.lf.speedy.Compiler

import scala.util.{Failure, Success, Try}

class ValueTranslatorSpecV2 extends ValueTranslatorSpec(LanguageMajorVersion.V2)

class ValueTranslatorSpec(majorLanguageVersion: LanguageMajorVersion)
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
    ParserParameters.defaultFor(majorLanguageVersion)

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
      ParserParameters(upgradablePkgId, LanguageVersion.v2_1)
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

  private[this] val compiledPackage = ConcurrentCompiledPackages(
    Compiler.Config.Default(majorLanguageVersion)
  )
  assert(compiledPackage.addPackage(defaultPackageId, pkg) == ResultDone.Unit)
  assert(compiledPackage.addPackage(upgradablePkgId, upgradablePkg) == ResultDone.Unit)

  "translateValue" should {

    val valueTranslator = new ValueTranslator(
      compiledPackage.pkgInterface,
      requireContractIdSuffix = false,
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
        ValueNumeric(Numeric.assertFromString("10.")),
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
        ValueRecord("", ImmArray("x" -> ValueInt64(33), "y" -> ValueText("a"))),
        SRecord("Mod:Tuple", ImmArray("x", "y"), ArrayList(SInt64(33), SText("a"))),
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

    "succeeds on well type values" in {
      forAll(testCases ++ emptyTestCase) { (typ, value, svalue) =>
        Try(unsafeTranslateValue(typ, value)) shouldBe Success(svalue)
      }
    }

    "handle different representation of the same record" in {
      val typ = t"Mod:Tuple Int64 Text"
      val testCases = Table(
        "record",
        ValueRecord("Mod:Tuple", ImmArray("x" -> ValueInt64(33), "y" -> ValueText("a"))),
        ValueRecord("Mod:Tuple", ImmArray("y" -> ValueText("a"), "x" -> ValueInt64(33))),
        ValueRecord("", ImmArray("x" -> ValueInt64(33), "y" -> ValueText("a"))),
        ValueRecord("", ImmArray("" -> ValueInt64(33), "" -> ValueText("a"))),
      )
      val svalue = SRecord("Mod:Tuple", ImmArray("x", "y"), ArrayList(SInt64(33), SText("a")))

      forEvery(testCases)(testCase =>
        Try(unsafeTranslateValue(typ, testCase)) shouldBe Success(svalue)
      )
    }

    "handle different representation of the same static record with upgrades enabled" in {
      val typ = t"Mod:Tuple Int64 Text"
      val testCases = Table(
        "record",
        ValueRecord("Mod:Tuple", ImmArray("x" -> ValueInt64(33), "y" -> ValueText("a"))),
        ValueRecord("Mod:Tuple", ImmArray("y" -> ValueText("a"), "x" -> ValueInt64(33))),
        ValueRecord("", ImmArray("x" -> ValueInt64(33), "y" -> ValueText("a"))),
        ValueRecord("", ImmArray("" -> ValueInt64(33), "" -> ValueText("a"))),
      )
      val svalue = SRecord("Mod:Tuple", ImmArray("x", "y"), ArrayList(SInt64(33), SText("a")))

      forEvery(testCases)(testCase =>
        Try(unsafeTranslateValue(typ, testCase)) shouldBe Success(svalue)
      )
    }

    val TRecordUpgradable =
      t"Mod:Record Int64 Text Party Unit"

    val TVariantUpgradable =
      t"Mod:Variant Int64 Text"

    val TEnumUpgradable =
      t"Mod:Enum"

    "return proper mismatch error for upgrades" in {
      val testCases = Table[Ast.Type, Value, PartialFunction[Error.Preprocessing.Error, _]](
        ("type", "value", "error"),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray(
              "fieldA" -> aInt,
              "fieldB" -> someParty, // Here the field has type Party instead of Text
              "fieldC" -> none,
            ),
          ),
          { case Error.Preprocessing.TypeMismatch(typ, value, _) =>
            typ shouldBe t"Text"
            value shouldBe aParty
          },
        ),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray( // fields non-order and non fully labelled.
              "fieldA" -> aInt,
              "" -> none,
              "fieldB" -> someText,
            ),
          ),
          { case Error.Preprocessing.TypeMismatch(typ, _, _) =>
            typ shouldBe TRecordUpgradable
          },
        ),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray(), // missing a non-optional field
          ),
          { case Error.Preprocessing.TypeMismatch(typ, _, _) =>
            typ shouldBe TRecordUpgradable
          },
        ),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray(
              "fieldA" -> aInt,
              "fieldB" -> someText,
              "fieldC" -> someParty,
              "fieldD" -> aInt, // extra non-optional field
            ),
          ),
          { case Error.Preprocessing.TypeMismatch(typ, _, _) =>
            typ shouldBe TRecordUpgradable
          },
        ),
        (
          TVariantUpgradable,
          ValueVariant("", "ConsB", aInt), // Here the variant has type Text instead of Int64
          { case Error.Preprocessing.TypeMismatch(typ, value, _) =>
            typ shouldBe t"Text"
            value shouldBe aInt
          },
        ),
        (
          TVariantUpgradable,
          ValueVariant("", "ConsC", aInt), // ConsC is not a constructor of Mod:Variant
          {
            case Error.Preprocessing.Lookup(
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
            case Error.Preprocessing.Lookup(
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
            case Error.Preprocessing.Lookup(
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
            case Error.Preprocessing.Lookup(
                  LookupError.NotFound(
                    Reference.DataEnumConstructor(_, consName),
                    Reference.DataEnumConstructor(_, _),
                  )
                ) =>
              consName shouldBe "Cons3"
          },
        ),
      )
      forEvery(testCases)((typ, value, _) =>
        inside(Try(unsafeTranslateValue(typ, value))) {
          case Failure(_: Error.Preprocessing.Error) =>
            ()
          // checkError(error)
        }
      )
    }

    "handle different representation of the same upgraded/downgraded record" in {
      val typ = t"Mod:Upgradeable"
      def sValue(extraFieldDefined: Boolean, anotherExtraFieldDefined: Boolean) =
        SRecord(
          "Mod:Upgradeable",
          ImmArray("field", "extraField", "anotherExtraField"),
          ArrayList(
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
        (Failure(Error.Preprocessing.TypeMismatch(typ, value, s)), value)
      val testCases = Table(
        ("svalue", "record"),
        upgradeCaseSuccess(
          true,
          true,
          ValueRecord(
            "Mod:Upgradeable",
            ImmArray(
              "field" -> ValueInt64(1),
              "extraField" -> ValueOptional(Some(ValueText("a"))),
              "anotherExtraField" -> ValueOptional(Some(ValueText("b"))),
            ),
          ),
        ),
        upgradeCaseSuccess(
          false,
          true,
          ValueRecord(
            "Mod:Upgradeable",
            ImmArray(
              "field" -> ValueInt64(1),
              "extraField" -> ValueOptional(None),
              "anotherExtraField" -> ValueOptional(Some(ValueText("b"))),
            ),
          ),
        ),
        upgradeCaseSuccess(
          false,
          true,
          ValueRecord(
            "Mod:Upgradeable",
            ImmArray(
              "field" -> ValueInt64(1),
              "anotherExtraField" -> ValueOptional(Some(ValueText("b"))),
              "extraField" -> ValueOptional(None),
            ),
          ),
        ),
        upgradeCaseSuccess(
          false,
          true,
          ValueRecord(
            "Mod:Upgradeable",
            ImmArray(
              "field" -> ValueInt64(1),
              "anotherExtraField" -> ValueOptional(Some(ValueText("b"))),
            ),
          ),
        ),
        upgradeCaseSuccess(
          false,
          false,
          ValueRecord(
            "Mod:Upgradeable",
            ImmArray(
              "field" -> ValueInt64(1)
            ),
          ),
        ),
        upgradeCaseSuccess(
          false,
          false,
          ValueRecord(
            "Mod:Upgradeable",
            ImmArray(
              "field" -> ValueInt64(1),
              "bonusField" -> ValueOptional(None),
            ),
          ),
        ),
        upgradeCaseSuccess(
          false,
          true,
          ValueRecord(
            "Mod:Upgradeable",
            ImmArray(
              "field" -> ValueInt64(1),
              "bonusField" -> ValueOptional(None),
              "anotherExtraField" -> ValueOptional(Some(ValueText("b"))),
            ),
          ),
        ),
        upgradeCaseFailure(
          "An optional contract field (\"bonusField\") with a value of Some may not be dropped during downgrading.",
          ValueRecord(
            "Mod:Upgradeable",
            ImmArray(
              "field" -> ValueInt64(1),
              "bonusField" -> ValueOptional(Some(ValueText("bad"))),
            ),
          ),
        ),
        upgradeCaseFailure(
          "Found non-optional extra field \"bonusField\", cannot remove for downgrading.",
          ValueRecord(
            "Mod:Upgradeable",
            ImmArray(
              "field" -> ValueInt64(1),
              "bonusField" -> ValueText("bad"),
            ),
          ),
        ),
        upgradeCaseFailure(
          "Missing non-optional field \"field\", cannot upgrade non-optional fields.",
          ValueRecord("Mod:Upgradeable", ImmArray()),
        ),
      )

      forEvery(testCases)((result, value) => Try(unsafeTranslateValue(typ, value)) shouldBe result)
    }

    "handle different representation of the same variant" in {
      val typ = t"Mod:Either Text Int64"
      val testCases = Table(
        "variant",
        ValueVariant("Mod:Either", "Left", ValueText("some test")),
        ValueVariant("", "Left", ValueText("some test")),
      )
      val svalue = SVariant("Mod:Either", "Left", 0, SText("some test"))

      forEvery(testCases)(value => Try(unsafeTranslateValue(typ, value)) shouldBe Success(svalue))
    }

    "handle different representation of the same enum" in {
      val typ = t"Mod:Color"
      val testCases = Table("enum", ValueEnum("Mod:Color", "green"), ValueEnum("", "green"))
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
              "x" -> ValueInt64(33),
              "y" -> ValueParty("Alice"), // Here the field has type Party instead of Text
            ),
          ),
        )
      )
      inside(res) { case Failure(Error.Preprocessing.TypeMismatch(typ, value, _)) =>
        typ shouldBe t"Text"
        value shouldBe ValueParty("Alice")
      }
    }

    "fails on non-well type values" in {
      forAll(testCases) { (typ1, value1, _) =>
        forAll(testCases) { (_, value2, _) =>
          if (value1 != value2) {
            a[Error.Preprocessing.Error] shouldBe thrownBy(
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
      val failure = Failure(Error.Preprocessing.ValueNesting(tooBig))

      Try(unsafeTranslateValue(t"Mod:MyList", notTooBig)) shouldBe a[Success[_]]
      Try(unsafeTranslateValue(t"Mod:MyList", tooBig)) shouldBe failure
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
        requireContractIdSuffix = false,
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

    "reject non suffixed V1 Contract IDs when requireV1ContractIdSuffix is true" in {

      val valueTranslator = new ValueTranslator(
        compiledPackage.pkgInterface,
        requireContractIdSuffix = true,
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
        Failure(Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(illegalCidV1))
      val failureV2 =
        Failure(Error.Preprocessing.IllegalContractId.NonSuffixV2ContractId(illegalCidV2))

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

  }

}
