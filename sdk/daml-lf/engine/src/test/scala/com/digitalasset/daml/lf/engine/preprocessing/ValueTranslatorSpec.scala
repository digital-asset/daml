// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package preprocessing

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion}
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
          variant @serializable MyList = MyNil : Unit | MyCons: Mod:MyCons ;

          record @serializable Template = { field : Int64 };
          record @serializable TemplateRef = { owner: Party, cid: (ContractId Mod:Template) };

          record @serializable Upgradeable = { field: Int64, extraField: (Option Text), anotherExtraField: (Option Text) };
        }
    """

  private[this] val compiledPackage = ConcurrentCompiledPackages(
    Compiler.Config.Default(majorLanguageVersion)
  )
  assert(compiledPackage.addPackage(defaultPackageId, pkg) == ResultDone.Unit)

  val valueTranslator = new ValueTranslator(
    compiledPackage.pkgInterface,
    requireV1ContractIdSuffix = false,
  )
  import valueTranslator.unsafeTranslateValue

  val nonEmptyTestCases = Table[Ast.Type, Value, List[Value], speedy.SValue](
    ("type", "normalized value", "non normalized value", "svalue"),
    (
      TUnit,
      ValueUnit,
      List.empty,
      SValue.Unit,
    ),
    (
      TBool,
      ValueTrue,
      List.empty,
      SValue.True,
    ),
    (
      TInt64,
      ValueInt64(42),
      List.empty,
      SInt64(42),
    ),
    (
      TTimestamp,
      ValueTimestamp(Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")),
      List.empty,
      STimestamp(Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")),
    ),
    (
      TDate,
      ValueDate(Time.Date.assertFromString("1879-03-14")),
      List.empty,
      SDate(Time.Date.assertFromString("1879-03-14")),
    ),
    (
      TText,
      ValueText("daml"),
      List.empty,
      SText("daml"),
    ),
    (
      TNumeric(Ast.TNat(Numeric.Scale.assertFromInt(10))),
      ValueNumeric(Numeric.assertFromString("10.0000000000")),
      List(
        ValueNumeric(Numeric.assertFromString("10.")),
        ValueNumeric(Numeric.assertFromString("10.0")),
        ValueNumeric(Numeric.assertFromString("10.00000000000000000000")),
      ),
      SNumeric(Numeric.assertFromString("10.0000000000")),
    ),
    (
      TParty,
      ValueParty("Alice"),
      List.empty,
      SParty("Alice"),
    ),
    (
      TContractId(t"Mod:Template"),
      ValueContractId(aCid),
      List.empty,
      SContractId(aCid),
    ),
    (
      TList(TText),
      ValueList(FrontStack(ValueText("a"), ValueText("b"))),
      List.empty,
      SList(FrontStack(SText("a"), SText("b"))),
    ),
    (
      TTextMap(TBool),
      ValueTextMap(SortedLookupList(Map("0" -> ValueTrue, "1" -> ValueFalse))),
      List.empty,
      SMap(true, SText("0") -> SValue.True, SText("1") -> SValue.False),
    ),
    (
      TGenMap(TInt64, TText),
      ValueGenMap(ImmArray(ValueInt64(1) -> ValueText("1"), ValueInt64(42) -> ValueText("42"))),
      List(
        ValueGenMap(ImmArray(ValueInt64(42) -> ValueText("42"), ValueInt64(1) -> ValueText("1"))),
        ValueGenMap(
          ImmArray(
            ValueInt64(1) -> ValueText("0"),
            ValueInt64(42) -> ValueText("42"),
            ValueInt64(1) -> ValueText("1"),
          )
        ),
      ),
      SMap(false, SInt64(1) -> SText("1"), SInt64(42) -> SText("42")),
    ),
    (
      TOptional(TText),
      ValueOptional(Some(ValueText("text"))),
      List.empty,
      SOptional(Some(SText("text"))),
    ),
    (
      t"Mod:Tuple Int64 Text",
      ValueRecord("", ImmArray("" -> ValueInt64(33), "" -> ValueText("a"))),
      List(
        ValueRecord("Mod:Tuple", ImmArray("x" -> ValueInt64(33), "y" -> ValueText("a"))),
        ValueRecord("Mod:Tuple", ImmArray("y" -> ValueText("a"), "x" -> ValueInt64(33))),
        ValueRecord("", ImmArray("x" -> ValueInt64(33), "y" -> ValueText("a"))),
        ValueRecord("Mod:Tuple", ImmArray("" -> ValueInt64(33), "y" -> ValueText("a"))),
        ValueRecord("", ImmArray("y" -> ValueText("a"), "x" -> ValueInt64(33))),
        ValueRecord("", ImmArray("" -> ValueInt64(33), "" -> ValueText("a"), "" -> ValueNone)),
      ),
      SRecord("Mod:Tuple", ImmArray("x", "y"), ArrayList(SInt64(33), SText("a"))),
    ),
    (
      t"Mod:Either Int64 Text",
      ValueVariant("", "Right", ValueText("some test")),
      List(
        ValueVariant("Mod:Either", "Right", ValueText("some test"))
      ),
      SVariant("Mod:Either", "Right", 1, SText("some test")),
    ),
    (
      Ast.TTyCon("Mod:Color"),
      ValueEnum("", "blue"),
      List(
        ValueEnum("Mod:Color", "blue")
      ),
      SEnum("Mod:Color", "blue", 2),
    ),
  )

  val emptyTestCase = Table[Ast.Type, Value, List[Nothing], speedy.SValue](
    ("type", "normalized value", "non normalized value", "svalue"),
    (
      TList(TText),
      ValueList(FrontStack.empty),
      List.empty,
      SList(FrontStack.empty),
    ),
    (
      TOptional(TText),
      ValueOptional(None),
      List.empty,
      SOptional(None),
    ),
    (
      TTextMap(TText),
      ValueTextMap(SortedLookupList.Empty),
      List.empty,
      SMap(true),
    ),
    (
      TGenMap(TInt64, TText),
      ValueGenMap(ImmArray.empty),
      List.empty,
      SMap(false),
    ),
  )

  "strict translateValue" should {
    "succeeds on normalized well-typed values" in {
      forEvery(nonEmptyTestCases ++ emptyTestCase) { (typ, v, _, svalue) =>
        Try(unsafeTranslateValue(typ, v, strict = true)) shouldBe Success(svalue)
      }
    }

    "failed on non-normalized " in {
      forEvery(nonEmptyTestCases ++ emptyTestCase) { (typ, _, nonNormal, _) =>
        nonNormal.foreach { v =>
          Try(unsafeTranslateValue(typ, v, strict = true)) shouldBe a[Failure[_]]
        }
      }
    }
  }

  "lenient translateValue" should {

    "succeeds on any well-typed values" in {
      forEvery(nonEmptyTestCases ++ emptyTestCase) { (typ, normal, nonNormal, svalue) =>
        (normal +: nonNormal).foreach { v =>
          Try(unsafeTranslateValue(typ, v, strict = false)) shouldBe Success(svalue)
        }
      }
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

      forEvery(testCases)((result, value) =>
        Try(unsafeTranslateValue(typ, value, strict = false)) shouldBe result
      )
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
          strict = false,
        )
      )
      inside(res) { case Failure(Error.Preprocessing.TypeMismatch(typ, value, _)) =>
        typ shouldBe t"Text"
        value shouldBe ValueParty("Alice")
      }
    }

    "fails on non-well type values" in {
      forAll(nonEmptyTestCases) { (typ1, value1, _, _) =>
        forAll(nonEmptyTestCases) { (_, value2, _, _) =>
          if (value1 != value2) {
            a[Error.Preprocessing.Error] shouldBe thrownBy(
              unsafeTranslateValue(typ1, value2, strict = false)
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

      Try(unsafeTranslateValue(t"Mod:MyList", notTooBig, strict = true)) shouldBe a[Success[_]]
      Try(unsafeTranslateValue(t"Mod:MyList", tooBig, strict = true)) shouldBe failure
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
        requireV1ContractIdSuffix = false,
      )
      val cids = List(
        ContractId.V1
          .assertBuild(
            crypto.Hash.hashPrivateKey("a suffixed V1 Contract ID"),
            Bytes.assertFromString("00"),
          ),
        ContractId.V1
          .assertBuild(crypto.Hash.hashPrivateKey("a non-suffixed V1 Contract ID"), Bytes.Empty),
      )

      cids.foreach(cid =>
        forEvery(testCasesForCid(cid))((typ, value) =>
          Try(valueTranslator.unsafeTranslateValue(typ, value, strict = true)) shouldBe a[Success[
            _
          ]]
        )
      )
    }

    "reject non suffixed V1 Contract IDs when requireV1ContractIdSuffix is true" in {

      val valueTranslator = new ValueTranslator(
        compiledPackage.pkgInterface,
        requireV1ContractIdSuffix = true,
      )
      val legalCid =
        ContractId.V1.assertBuild(
          crypto.Hash.hashPrivateKey("a legal Contract ID"),
          Bytes.assertFromString("00"),
        )
      val illegalCid =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("an illegal Contract ID"), Bytes.Empty)
      val failure = Failure(Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(illegalCid))

      forEvery(testCasesForCid(legalCid))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value, strict = true)) shouldBe a[Success[_]]
      )
      forEvery(testCasesForCid(illegalCid))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value, strict = true)) shouldBe failure
      )
    }

  }

}
