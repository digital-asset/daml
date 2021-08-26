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
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

class PreprocessorSpec
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  import Preprocessor.ArrayList
  import defaultParserParameters.{defaultPackageId => pkgId}

  private implicit def toName(s: String): Ref.Name = Ref.Name.assertFromString(s)

  private[this] val recordCon =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Record"))
  private[this] val recordRefCon =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:RecordRef"))
  private[this] val variantCon =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Either"))
  private[this] val enumCon =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Enum"))
  private[this] val tricky =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Tricky"))
  private[this] val myListTyCons =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:MyList"))
  private[this] val myNilCons = Ref.Name.assertFromString("MyNil")
  private[this] val myConsCons = Ref.Name.assertFromString("MyCons")
  private[this] val alice = Ref.Party.assertFromString("Alice")
  private[this] val dummySuffix = Bytes.assertFromString("00")
  private[this] val typ = t"ContractId Mod:Record"
  private[this] val aCid =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("a Contract ID"), dummySuffix)

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

          val @noPartyLiterals toParties: Mod:RecordRef -> List Party =
            \ (ref: Mod:RecordRef) -> Cons @Party [Mod:RecordRef {owner} ref] (Nil @Party);

          template (this : RecordRef) = {
            precondition True,
            signatories Mod:toParties this,
            observers Mod:toParties this,
            agreement "Agreement",
            choices {
              choice Change (self) (newCid: ContractId Mod:Record) : ContractId Mod:RecordRef,
                  controllers Mod:toParties this,
                  observers Nil @Party
                to create @Mod:RecordRef Mod:RecordRef { owner = Mod:RecordRef {owner} this, cid = newCid }
            },
            key @Party (Mod:RecordRef {owner} this) (\ (p: Party) -> Cons @Party [p] (Nil @Party))
          };

        }

    """

  private[this] val compiledPackage = ConcurrentCompiledPackages()
  assert(compiledPackage.addPackage(pkgId, pkg) == ResultDone.Unit)

  "translateValue" should {

    val valueTranslator = new ValueTranslator(
      compiledPackage.interface,
      requireV1ContractId = true,
      requireV1ContractIdSuffix = false,
    )
    import valueTranslator.unsafeTranslateValue

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
      (TParty, ValueParty(alice), SParty(alice)),
      (
        TContractId(Ast.TTyCon(recordCon)),
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
        Ast.TTyCon(recordCon),
        ValueRecord(None, ImmArray(Some[Ref.Name]("field") -> ValueInt64(33))),
        SRecord(recordCon, ImmArray("field"), ArrayList(SInt64(33))),
      ),
      (
        TTyConApp(variantCon, ImmArray(TText, TInt64)),
        ValueVariant(None, "Left", ValueText("some test")),
        SVariant(variantCon, "Left", 0, SText("some test")),
      ),
      (Ast.TTyCon(enumCon), ValueEnum(None, "value1"), SEnum(enumCon, "value1", 0)),
      (
        Ast.TApp(Ast.TTyCon(tricky), Ast.TBuiltin(Ast.BTList)),
        ValueRecord(None, ImmArray(None -> ValueNil)),
        SRecord(tricky, ImmArray("x"), ArrayList(SValue.EmptyList)),
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
      val failure = Failure(Error.Preprocessing.ValueNesting(tooBig))

      Try(unsafeTranslateValue(Ast.TTyCon(myListTyCons), notTooBig)) shouldBe a[Success[_]]
      Try(unsafeTranslateValue(Ast.TTyCon(myListTyCons), tooBig)) shouldBe failure
    }

    def testCasesForCid(culprit: ContractId) = {
      val cid = ValueContractId(culprit)
      Table[Ast.Type, Value[ContractId]](
        ("type" -> "value"),
        t"ContractId Mod:Record" -> cid,
        TList(typ) -> ValueList(FrontStack(cid)),
        TTextMap(typ) -> ValueTextMap(SortedLookupList(Map("0" -> cid))),
        TGenMap(TInt64, typ) -> ValueGenMap(ImmArray(ValueInt64(1) -> cid)),
        TGenMap(typ, TInt64) -> ValueGenMap(ImmArray(cid -> ValueInt64(0))),
        TOptional(typ) -> ValueOptional(Some(cid)),
        Ast.TTyCon(recordRefCon) -> ValueRecord(
          None,
          ImmArray(None -> ValueParty(alice), None -> cid),
        ),
        TTyConApp(variantCon, ImmArray(typ, TInt64)) -> ValueVariant(
          None,
          "Left",
          cid,
        ),
      )
    }

    "accept all contract IDs when require flags are false" in {

      val valueTranslator = new ValueTranslator(
        compiledPackage.interface,
        requireV1ContractId = false,
        requireV1ContractIdSuffix = false,
      )
      val cids = List(
        ContractId.V1
          .assertBuild(crypto.Hash.hashPrivateKey("a suffixed V1 Contract ID"), dummySuffix),
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
        requireV1ContractId = true,
        requireV1ContractIdSuffix = false,
      )
      val legalCid =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("a legal Contract ID"), dummySuffix)
      val illegalCid =
        ContractId.V0.assertFromString("#illegal Contract ID")
      val failure = Failure(Error.Preprocessing.IllegalContractId(illegalCid))

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
        requireV1ContractId = true,
        requireV1ContractIdSuffix = true,
      )
      val legalCid =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("a legal Contract ID"), dummySuffix)
      val illegalCid =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("an illegal Contract ID"), Bytes.Empty)
      val failure = Failure(Error.Preprocessing.IllegalContractId(illegalCid))

      forEvery(testCasesForCid(legalCid))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe a[Success[_]]
      )
      forEvery(testCasesForCid(illegalCid))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe failure
      )
    }

  }

  "preprocessCommand" should {
    import command._

    def payload(cid: ContractId) = ValueRecord(
      None,
      ImmArray(None -> ValueParty(alice), None -> ValueContractId(cid)),
    )

    def testCases(culpritCid: ContractId, innocentCid: ContractId) = Table[ApiCommand](
      "command",
      CreateCommand(
        recordRefCon,
        payload(culpritCid),
      ),
      ExerciseCommand(
        recordRefCon,
        innocentCid,
        "Change",
        ValueContractId(culpritCid),
      ),
      ExerciseCommand(
        recordRefCon,
        culpritCid,
        "Change",
        ValueContractId(innocentCid),
      ),
      CreateAndExerciseCommand(
        recordRefCon,
        payload(culpritCid),
        "Change",
        ValueContractId(innocentCid),
      ),
      CreateAndExerciseCommand(
        recordRefCon,
        payload(innocentCid),
        "Change",
        ValueContractId(culpritCid),
      ),
      ExerciseByKeyCommand(
        recordRefCon,
        ValueParty(alice),
        "Change",
        ValueContractId(culpritCid),
      ),
    )

    "accept all contract IDs when require flags are false" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.interface,
        requireV1ContractId = false,
        requireV1ContractIdSuffix = false,
      )

      val cids = List(
        ContractId.V1
          .assertBuild(crypto.Hash.hashPrivateKey("a suffixed V1 Contract ID"), dummySuffix),
        ContractId.V1
          .assertBuild(crypto.Hash.hashPrivateKey("a non-suffixed V1 Contract ID"), Bytes.Empty),
        ContractId.V0.assertFromString("#a V0 Contract ID"),
      )

      cids.foreach(cid =>
        forEvery(testCases(cids.head, cid))(cmd =>
          Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe a[Success[_]]
        )
      )

    }

    "reject V0 Contract IDs when requireV1ContractId flag is true" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.interface,
        requireV1ContractId = true,
        requireV1ContractIdSuffix = false,
      )

      val List(aLegalCid, anotherLegalCid) =
        List("a legal Contract ID", "another legal Contract ID").map(s =>
          ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)
        )
      val illegalCid = ContractId.V0.assertFromString("#illegal Contract ID")
      val failure = Failure(Error.Preprocessing.IllegalContractId(illegalCid))

      forEvery(testCases(aLegalCid, anotherLegalCid))(cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe a[Success[_]]
      )

      forEvery(testCases(illegalCid, aLegalCid))(cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe failure
      )
    }

    "reject non suffixed V1 Contract IDs when requireV1ContractIdSuffix is true" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.interface,
        requireV1ContractId = true,
        requireV1ContractIdSuffix = true,
      )
      val List(aLegalCid, anotherLegalCid) =
        List("a legal Contract ID", "another legal Contract ID").map(s =>
          ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)
        )
      val illegalCid =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("an illegal Contract ID"), Bytes.Empty)
      val failure = Failure(Error.Preprocessing.IllegalContractId(illegalCid))

      forEvery(testCases(aLegalCid, anotherLegalCid)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe a[Success[_]]
      }
      forEvery(testCases(illegalCid, aLegalCid)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe failure
      }
    }

  }

}
