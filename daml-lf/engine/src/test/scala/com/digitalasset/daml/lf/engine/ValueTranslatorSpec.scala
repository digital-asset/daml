// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.language.Util._
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value
import com.daml.lf.value.Value._
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class ValueTranslatorSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import Preprocessor.ArrayList
  import com.daml.lf.testing.parser.Implicits._
  import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId => _, _}
  private implicit val defaultPackageId = defaultParserParameters.defaultPackageId

  val aCid =
    ContractId.V1.assertBuild(
      crypto.Hash.hashPrivateKey("a Contract ID"),
      Bytes.assertFromString("00"),
    )

  lazy val pkg =
    p"""
        module Mod {

          record @serializable Record = { field : Int64 };
          variant @serializable Either (a: *) (b: *) = Left : a | Right : b;
          enum Enum = value1 | value2;

          record Tricky (b: * -> *) = { x : b Unit };

          record MyCons = { head : Int64, tail: Mod:MyList };
          variant MyList = MyNil : Unit | MyCons: Mod:MyCons ;

          record @serializable RecordRef = { owner: Party, cid: (ContractId Mod:Record) };

        }
    """

  private[this] val compiledPackage = ConcurrentCompiledPackages()
  assert(compiledPackage.addPackage(defaultPackageId, pkg) == ResultDone.Unit)

  "translateValue" should {

    val valueTranslator = new ValueTranslator(
      compiledPackage.interface,
      forbidV0ContractId = true,
      requireV1ContractIdSuffix = false,
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
        TNumeric(Ast.TNat(Decimal.scale)),
        ValueNumeric(Numeric.assertFromString("10.")),
        SNumeric(Numeric.assertFromString("10.0000000000")),
      ),
//      TNumeric(TNat(9)) ,
//        ValueNumeric(Numeric.assertFromString("9.000000000")),
      (TParty, ValueParty("Alice"), SParty("Alice")),
      (
        TContractId(t"Mod:Record"),
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
        t"Mod:Record",
        ValueRecord("", ImmArray("field" -> ValueInt64(33))),
        SRecord("Mod:Record", ImmArray("field"), ArrayList(SInt64(33))),
      ),
      (
        t"Mod:Either Text Int64",
        ValueVariant("", "Left", ValueText("some test")),
        SVariant("Mod:Either", "Left", 0, SText("some test")),
      ),
      (Ast.TTyCon("Mod:Enum"), ValueEnum("", "value1"), SEnum("Mod:Enum", "value1", 0)),
      (
        Ast.TApp(Ast.TTyCon("Mod:Tricky"), Ast.TBuiltin(Ast.BTList)),
        ValueRecord("", ImmArray("" -> ValueNil)),
        SRecord("Mod:Tricky", ImmArray("x"), ArrayList(SValue.EmptyList)),
      ),
    )

    "succeeds on well type values" in {
      forAll(testCases) { (typ, value, svalue) =>
        unsafeTranslateValue(typ, value) shouldBe svalue
      }
    }

    "fails on non-well type values" in {
      forAll(testCases) { (typ1, value1, _) =>
        forAll(testCases) { (_, value2, _) =>
          if (value1 != value2) {
            a[Error.Preprocessing.Error] shouldBe thrownBy(unsafeTranslateValue(typ1, value2))
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
        t"ContractId Mod:Record" -> cid,
        TList(t"ContractId Mod:Record") -> ValueList(FrontStack(cid)),
        TTextMap(t"ContractId Mod:Record") -> ValueTextMap(SortedLookupList(Map("0" -> cid))),
        TGenMap(TInt64, t"ContractId Mod:Record") -> ValueGenMap(ImmArray(ValueInt64(1) -> cid)),
        TGenMap(t"ContractId Mod:Record", TInt64) -> ValueGenMap(ImmArray(cid -> ValueInt64(0))),
        TOptional(t"ContractId Mod:Record") -> ValueOptional(Some(cid)),
        Ast.TTyCon("Mod:RecordRef") -> ValueRecord(
          "",
          ImmArray("" -> ValueParty("Alice"), "" -> cid),
        ),
        TTyConApp("Mod:Either", ImmArray(t"ContractId Mod:Record", TInt64)) -> ValueVariant(
          "",
          "Left",
          cid,
        ),
      )
    }

    "accept all contract IDs when require flags are false" in {

      val valueTranslator = new ValueTranslator(
        compiledPackage.interface,
        forbidV0ContractId = false,
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
        ContractId.V0.assertFromString("#a V0 Contract ID"),
      )

      cids.foreach(cid =>
        forEvery(testCasesForCid(cid))((typ, value) =>
          Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe a[Success[_]]
        )
      )
    }

    "reject V0 Contract IDs when requireV1ContractId flag is true" in {

      val valueTranslator = new ValueTranslator(
        compiledPackage.interface,
        forbidV0ContractId = true,
        requireV1ContractIdSuffix = false,
      )
      val legalCid =
        ContractId.V1.assertBuild(
          crypto.Hash.hashPrivateKey("a legal Contract ID"),
          Bytes.assertFromString("00"),
        )
      val illegalCid =
        ContractId.V0.assertFromString("#illegal Contract ID")
      val failure = Failure(Error.Preprocessing.IllegalContractId.V0ContractId(illegalCid))

      forEvery(testCasesForCid(legalCid))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe a[Success[_]]
      )
      forEvery(testCasesForCid(illegalCid))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe failure
      )
    }

    "reject non suffixed V1 Contract IDs when requireV1ContractIdSuffix is true" in {

      val valueTranslator = new ValueTranslator(
        compiledPackage.interface,
        forbidV0ContractId = true,
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
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe a[Success[_]]
      )
      forEvery(testCasesForCid(illegalCid))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe failure
      )
    }

  }

}
