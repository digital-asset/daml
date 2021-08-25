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
  private[this] val cidWithSuffix =
    ContractId.V1(crypto.Hash.hashPrivateKey("cidWithSuffix"), dummySuffix)
  private[this] val cidWithoutSuffix =
    ContractId.V1(crypto.Hash.hashPrivateKey("cidWithoutSuffix"), Bytes.Empty)
  private[this] val typ = t"ContractId Mod:Record"

  private[this] def suffixValue(v: Value[ContractId]) =
    data.assertRight(v.suffixCid(_ => dummySuffix))

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
  private[this] val preprocessor = new Preprocessor(compiledPackage, requiredCidSuffix = true)
  import preprocessor.{translateValue, preprocessCommand}

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
      (TParty, ValueParty(alice), SParty(alice)),
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

    "reject non suffix Contract IDs" in {

      val cidValueWithoutSuffix = ValueContractId(cidWithoutSuffix)

      val testCases = Table[Ast.Type, Value[ContractId]](
        ("type" -> "value"),
        t"ContractId Mod:Record" -> cidValueWithoutSuffix,
        TList(typ) -> ValueList(FrontStack(cidValueWithoutSuffix)),
        TTextMap(typ) -> ValueTextMap(SortedLookupList(Map("0" -> cidValueWithoutSuffix))),
        TGenMap(TInt64, typ) -> ValueGenMap(ImmArray(ValueInt64(1) -> cidValueWithoutSuffix)),
        TGenMap(typ, TInt64) -> ValueGenMap(ImmArray(cidValueWithoutSuffix -> ValueInt64(0))),
        TOptional(typ) -> ValueOptional(Some(cidValueWithoutSuffix)),
        Ast.TTyCon(recordRefCon) -> ValueRecord(
          None,
          ImmArray(None -> ValueParty(alice), None -> cidValueWithoutSuffix),
        ),
        TTyConApp(variantCon, ImmArray(typ, TInt64)) -> ValueVariant(
          None,
          "Left",
          cidValueWithoutSuffix,
        ),
      )

      forAll(testCases) { (typ, value) =>
        translateValue(typ, suffixValue(value)) shouldBe a[ResultDone[_]]
        translateValue(typ, value) shouldBe a[ResultError]
      }
    }
  }

  "preprocessCommand" should {

    import command._

    def suffixCid(cid: ContractId) =
      cid match {
        case ContractId.V1(discriminator, suffix) if suffix.isEmpty =>
          ContractId.V1(discriminator, dummySuffix)
        case otherwise =>
          otherwise
      }

    def suffixCmd(cmd: ApiCommand) =
      cmd match {
        case CreateCommand(templateId, argument) =>
          CreateCommand(templateId, suffixValue(argument))
        case ExerciseCommand(templateId, contractId, choiceId, argument) =>
          ExerciseCommand(templateId, suffixCid(contractId), choiceId, suffixValue(argument))
        case ExerciseByKeyCommand(templateId, contractKey, choiceId, argument) =>
          ExerciseByKeyCommand(templateId, contractKey, choiceId, suffixValue(argument))
        case CreateAndExerciseCommand(templateId, createArgument, choiceId, choiceArgument) =>
          CreateAndExerciseCommand(
            templateId,
            suffixValue(createArgument),
            choiceId,
            suffixValue(choiceArgument),
          )
      }

    "reject non suffix Contract IDs" in {

      val key = ValueParty(alice)
      val payloadWithoutSuffix = ValueRecord(
        None,
        ImmArray(None -> ValueParty(alice), None -> ValueContractId(cidWithoutSuffix)),
      )
      val payloadWithSuffix = ValueRecord(
        None,
        ImmArray(None -> ValueParty(alice), None -> ValueContractId(cidWithSuffix)),
      )

      val testCases = Table[ApiCommand](
        "command",
        CreateCommand(
          recordRefCon,
          payloadWithoutSuffix,
        ),
        ExerciseCommand(
          recordRefCon,
          cidWithSuffix,
          "Change",
          ValueContractId(cidWithoutSuffix),
        ),
        ExerciseCommand(
          recordRefCon,
          cidWithoutSuffix,
          "Change",
          ValueContractId(cidWithSuffix),
        ),
        CreateAndExerciseCommand(
          recordRefCon,
          payloadWithoutSuffix,
          "Change",
          ValueContractId(cidWithSuffix),
        ),
        CreateAndExerciseCommand(
          recordRefCon,
          payloadWithSuffix,
          "Change",
          ValueContractId(cidWithoutSuffix),
        ),
        ExerciseByKeyCommand(
          recordRefCon,
          key,
          "Change",
          ValueContractId(cidWithoutSuffix),
        ),
      )

      forAll(testCases) { cmd =>
        preprocessCommand(suffixCmd(cmd)) shouldBe a[ResultDone[_]]
        preprocessCommand(cmd) shouldBe a[ResultError]
      }

    }

  }

}
