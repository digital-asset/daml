// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.language.Util._
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.value.Value
import com.daml.lf.value.Value._
import org.scalatest.Inside
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

class PreprocessorSpec
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  import Preprocessor.ArrayList

  import defaultParserParameters.{defaultPackageId => pkgId}

  private implicit def toName(s: String): Ref.Name = Ref.Name.assertFromString(s)

  private[this] val recordCon =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Record"))
  private[this] val variantCon =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Variant"))
  private[this] val enumCon =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Enum"))
  private[this] val tricky =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Tricky"))
  private[this] val myListTyCons =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:MyList"))
  private[this] val myNilCons = Ref.Name.assertFromString("MyNil")
  private[this] val myConsCons = Ref.Name.assertFromString("MyCons")

  val pkg =
    p"""
        module Module {

          record Record = { field : Int64 };
          variant Variant = variant1 : Text | variant2 : Int64;
          enum Enum = value1 | value2;

          record Tricky (b: * -> *) = { x : b Unit };

          record MyCons = { head : Int64, tail: Module:MyList };
          variant MyList = MyNil : Unit | MyCons: Module:MyCons ;

        }

    """

  "translateValue" should {

    val testCases = Table[Ast.Type, Value[ContractId], speedy.SValue](
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
//      TNumeric(TNat(9)) ,
//        ValueNumeric(Numeric.assertFromString("9.000000000")),
      (
        TParty,
        ValueParty(Ref.Party.assertFromString("Alice")),
        SParty(Ref.Party.assertFromString("Alice")),
      ),
      (
        TContractId(Ast.TTyCon(recordCon)),
        ValueContractId(ContractId.assertFromString("#contractId")),
        SContractId(ContractId.assertFromString("#contractId")),
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
        Ast.TTyCon(recordCon),
        ValueRecord(None, ImmArray(Some[Ref.Name]("field") -> ValueInt64(33))),
        SRecord(recordCon, ImmArray("field"), ArrayList(SInt64(33))),
      ),
      (
        Ast.TTyCon(variantCon),
        ValueVariant(None, "variant1", ValueText("some test")),
        SVariant(variantCon, "variant1", 0, SText("some test")),
      ),
      (Ast.TTyCon(enumCon), ValueEnum(None, "value1"), SEnum(enumCon, "value1", 0)),
      (
        Ast.TApp(Ast.TTyCon(tricky), Ast.TBuiltin(Ast.BTList)),
        ValueRecord(None, ImmArray(None -> ValueNil)),
        SRecord(tricky, ImmArray("x"), ArrayList(SValue.EmptyList)),
      ),
    )

    val compiledPackage = ConcurrentCompiledPackages()
    assert(compiledPackage.addPackage(pkgId, pkg) == ResultDone.Unit)
    val preprocessor = new Preprocessor(compiledPackage)
    import preprocessor.translateValue

    "succeeds on well type values" in {
      forAll(testCases) { (typ, value, svalue) =>
        translateValue(typ, value) shouldBe ResultDone(svalue)
      }
    }

    "fails on non-well type values" in {
      forAll(testCases) { (typ1, value1, _) =>
        forAll(testCases) { (_, value2, _) =>
          if (value1 != value2)
            translateValue(typ1, value2) shouldBe a[ResultError]
        }
      }
    }

    "fails on too deep values" in {

      def mkMyList(n: Int) =
        Iterator.range(0, n).foldLeft[Value[Nothing]](ValueVariant(None, myNilCons, ValueUnit)) {
          case (v, n) =>
            ValueVariant(
              None,
              myConsCons,
              ValueRecord(None, ImmArray(None -> ValueInt64(n.toLong), None -> v)),
            )
        }

      val notTooBig = mkMyList(49)
      val tooBig = mkMyList(50)

      translateValue(Ast.TTyCon(myListTyCons), notTooBig) shouldBe a[ResultDone[_]]
      inside(translateValue(Ast.TTyCon(myListTyCons), tooBig)) {
        case ResultError(Error.Preprocessing(err)) =>
          err shouldBe Error.Preprocessing.ValueNesting(tooBig)
      }
    }
  }

}
