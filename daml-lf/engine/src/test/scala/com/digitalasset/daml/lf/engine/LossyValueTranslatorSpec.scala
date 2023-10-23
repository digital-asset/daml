// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.engine.preprocessing.LossyValueTranslator.{
  lossyConversionFieldPrefix,
  lossyConversionIdentifier,
  lossyConversionVariantRank,
  unsafeTranslate,
}
import com.daml.lf.language.Ast.TTyCon
import com.daml.lf.language.Util._
import com.daml.lf.language.{Ast, LanguageMajorVersion}
import com.daml.lf.speedy.ArrayList
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.value.Value
import com.daml.lf.value.Value._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class LossyValueTranslatorSpec()
    extends AnyWordSpec
    with Inside
    with Matchers
    with TableDrivenPropertyChecks {

  import com.daml.lf.testing.parser.Implicits.SyntaxHelper
  import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId => _, _}

  private[this] implicit val parserParameters
      : ParserParameters[LossyValueTranslatorSpec.this.type] =
    ParserParameters.defaultFor(LanguageMajorVersion.V2)

  private[this] implicit val defaultPackageId: Ref.PackageId =
    parserParameters.defaultPackageId

  private[this] val aCid =
    ContractId.V1.assertBuild(
      crypto.Hash.hashPrivateKey("a Contract ID"),
      Bytes.assertFromString("00"),
    )

  "translateValue" should {

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
        TNumeric(Ast.TNat(Decimal.scale)),
        ValueNumeric(Numeric.assertFromString("10.")),
        SNumeric(Numeric.assertFromString("10.0000000000")),
      ),
      (TParty, ValueParty("Alice"), SParty("Alice")),
      (
        TContractId(TTyCon(lossyConversionIdentifier)),
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
        SMap(true, Iterator(SText("0") -> SValue.True, SText("1") -> SValue.False)),
      ),
      (
        TGenMap(TInt64, TText),
        ValueGenMap(ImmArray(ValueInt64(1) -> ValueText("1"), ValueInt64(42) -> ValueText("42"))),
        SMap(false, Iterator(SInt64(1) -> SText("1"), SInt64(42) -> SText("42"))),
      ),
      (TOptional(TText), ValueOptional(Some(ValueText("text"))), SOptional(Some(SText("text")))),
      (
        t"'Lossy':Conversion:Id Int64 Text Bool",
        ValueRecord(
          "",
          ImmArray("x" -> ValueInt64(33), "y" -> ValueText("a"), "z" -> ValueBool(true)),
        ),
        SRecord(
          lossyConversionIdentifier,
          ImmArray("x", "y", "z"),
          ArrayList(SInt64(33), SText("a"), SBool(true)),
        ),
      ),
      (
        t"'Lossy':Conversion:Id Text",
        ValueVariant("", "Right", ValueText("some test")),
        SVariant(lossyConversionIdentifier, "Right", lossyConversionVariantRank, SText("some test")),
      ),
      (
        Ast.TTyCon(lossyConversionIdentifier),
        ValueEnum("", "blue"),
        SEnum(lossyConversionIdentifier, "blue", lossyConversionVariantRank),
      ),
      (
        Ast.TApp(Ast.TTyCon(lossyConversionIdentifier), Ast.TBuiltin(Ast.BTList)),
        ValueRecord("", ImmArray("" -> ValueNil)),
        SRecord(
          lossyConversionIdentifier,
          ImmArray(Ref.Name.assertFromString(s"${lossyConversionFieldPrefix}1")),
          ArrayList(SValue.EmptyList),
        ),
      ),
    )

    "succeeds on well type values" in {
      forAll(testCases) { (ty, value, sValue) =>
        Try(
          unsafeTranslate(value)
        ) shouldBe Success((sValue, ty))
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

      Try(
        unsafeTranslate(notTooBig)
      ) shouldBe a[Success[_]]
      Try(
        unsafeTranslate(tooBig)
      ) shouldBe failure
    }

  }

}
