// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data._
import com.daml.lf.language.Ast.{TNat, TTyCon}
import com.daml.lf.language.Util._
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.lf.value.Value._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

class ValueEnricherSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import defaultParserParameters.{defaultPackageId => pkgId}

  private implicit def toName(s: String): Ref.Name = Ref.Name.assertFromString(s)

  val pkg =
    p"""
        module Mod {

          record @serializable Record = { field : Int64 };
          variant @serializable Variant = variant1 : Text | variant2 : Int64 ;
          enum Enum = value1 | value2;

          record @serializable Key = {
             party: Party,
             idx: Int64
          };

          record @serializable Contract = {
            key: Mod:Key,
            cids: List (ContractId Mod:Contract)
          };

          val @noPartyLiterals keyParties: (Mod:Key -> List Party) =
            \(key: Mod:Key) ->
              Cons @Party [Mod:Key {party} key] (Nil @Party);

          val @noPartyLiterals contractParties : (Mod:Contract -> List Party) =
            \(contract: Mod:Contract) ->
              Mod:keyParties (Mod:Contract {key} contract);

          template (this : Contract) =  {
             precondition True,
             signatories Mod:contractParties this,
             observers Mod:contractParties this,
             agreement "Agreement",
             choices {
               choice @nonConsuming Noop (self) (r: Mod:Record) : Mod:Record,
                 controllers 
                   Mod:contractParties this
                 to
                   upure @Mod:Record r 
             },
             key @Mod:Key (Mod:Contract {key} this) Mod:keyParties
          };
        }

    """

  val recordCon = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Record"))
  val variantCon = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Variant"))
  val enumCon = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Enum"))

  val contractCon = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Contract"))
  val keyCon = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Key"))

  private[this] val engine = Engine.DevEngine()
  engine.preloadPackage(pkgId, pkg).consume(_ => None, _ => None, _ => None)

  private[this] val enricher = new ValueEnricher(engine)

  "enrichValue" should {

    val testCases = Table(
      ("type", "input", "expected output"),
      (TUnit, ValueUnit, ValueUnit),
      (TBool, ValueTrue, ValueTrue),
      (TInt64, ValueInt64(42), ValueInt64(42)),
      (
        TTimestamp,
        ValueTimestamp(Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")),
        ValueTimestamp(Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")),
      ),
      (
        TDate,
        ValueDate(Time.Date.assertFromString("1879-03-14")),
        ValueDate(Time.Date.assertFromString("1879-03-14")),
      ),
      (TText, ValueText("daml"), ValueText("daml")),
      (
        TNumeric(TNat(Decimal.scale)),
        ValueNumeric(Numeric.assertFromString("10.")),
        ValueNumeric(Numeric.assertFromString("10.0000000000")),
      ),
      (
        TParty,
        ValueParty(Ref.Party.assertFromString("Alice")),
        ValueParty(Ref.Party.assertFromString("Alice")),
      ),
      (
        TContractId(TTyCon(recordCon)),
        ValueContractId(ContractId.assertFromString("#contractId")),
        ValueContractId(ContractId.assertFromString("#contractId")),
      ),
      (
        TList(TText),
        ValueList(FrontStack(ValueText("a"), ValueText("b"))),
        ValueList(FrontStack(ValueText("a"), ValueText("b"))),
      ),
      (
        TTextMap(TBool),
        ValueTextMap(SortedLookupList(Map("0" -> ValueTrue, "1" -> ValueFalse))),
        ValueTextMap(SortedLookupList(Map("0" -> ValueTrue, "1" -> ValueFalse))),
      ),
      (
        TOptional(TText),
        ValueOptional(Some(ValueText("text"))),
        ValueOptional(Some(ValueText("text"))),
      ),
      (
        TTyCon(recordCon),
        ValueRecord(None, ImmArray(None -> ValueInt64(33))),
        ValueRecord10(recordCon, ImmArray("field"), ImmArray(ValueInt64(33))),
      ),
      (
        TTyCon(variantCon),
        ValueVariant(None, "variant1", ValueText("some test")),
        ValueVariant(Some(variantCon), "variant1", ValueText("some test")),
      ),
      (TTyCon(enumCon), ValueEnum(None, "value1"), ValueEnum(Some(enumCon), "value1")),
    )

    "enrich values as expected" in {
      forAll(testCases) { (typ, input, output) =>
        enricher.enrichValue(typ, input) shouldBe ResultDone(output)
      }
    }
  }

  "enrichTransaction" should {

    val alice = Ref.Party.assertFromString("Alice")

    def buildTransaction(
        contract: Value[ContractId],
        key: Value[ContractId],
        record: Value[ContractId],
    ) = {
      val builder = TransactionBuilder(TransactionVersion.minTypeErasure)
      val create =
        builder.create(
          id = "#01",
          template = s"$pkgId:Mod:Contract",
          argument = contract,
          signatories = Seq(alice),
          observers = Seq(alice),
          key = Some(key),
        )
      builder.add(create)
      builder.add(builder.fetch(create))
      builder.lookupByKey(create, true)
      builder.exercise(
        create,
        "Noop",
        false,
        Set(alice),
        record,
        Some(record),
      )
      builder.buildCommitted()
    }

    "enrich transaction as expected" in {

      val inputKey = ValueRecord(
        None,
        ImmArray(
          None -> ValueParty(alice),
          None -> Value.ValueInt64(0),
        ),
      )

      val inputContract =
        ValueRecord(
          None,
          ImmArray(
            None -> inputKey,
            None -> Value.ValueNil,
          ),
        )

      val inputRecord =
        ValueRecord(None, ImmArray(None -> ValueInt64(33)))

      val inputTransaction = buildTransaction(
        inputContract,
        inputKey,
        inputRecord,
      )

      val outputKey = ValueRecord10(
        keyCon,
        ImmArray("party", "idx"),
        ImmArray(ValueParty(alice), Value.ValueInt64(0)),
      )

      val outputContract =
        ValueRecord10(
          contractCon,
          ImmArray("key", "cids"),
          ImmArray(outputKey, Value.ValueNil),
        )

      val outputRecord = ValueRecord10(recordCon, ImmArray("field"), ImmArray(ValueInt64(33)))

      val outputTransaction = buildTransaction(
        outputContract,
        outputKey,
        outputRecord,
      )

      // enricher.enrichTransaction(inputTransaction) shouldNot be(ResultDone(inputTransaction))
      enricher.enrichTransaction(inputTransaction) shouldBe ResultDone(outputTransaction)

      // ResultDone(VersionedTransaction(VDev,Map(NodeId(0) -> NodeCreate(ContractId(#01),-pkgId-:Mod:Contract,ValueRecord10(-pkgId-:Mod:Contract,ImmArray(key,cids),ImmArray(ValueRecord10(-pkgId-:Mod:Key,ImmArray(party,idx),ImmArray(ValueParty(Alice),ValueInt64(0))),ValueList(FrontStack()))),,None,Set(Alice),Set(Alice),Some(KeyWithMaintainers(ValueRecord10(-pkgId-:Mod:Key,ImmArray(party,idx),ImmArray(ValueParty(Alice),ValueInt64(0))),Set(Alice))),VDev), NodeId(1) -> NodeFetch(ContractId(#01),-pkgId-:Mod:Contract,None,Set(Alice),Set(Alice),Set(Alice),Some(KeyWithMaintainers(ValueRecord10(-pkgId-:Mod:Key,ImmArray(party,idx),ImmArray(ValueParty(Alice),ValueInt64(0))),Set(Alice))),true,VDev)),ImmArray(NodeId(0),NodeId(1)))) was not equal to
      // ResultDone(VersionedTransaction(VDev,Map(NodeId(0) -> NodeCreate(ContractId(#01),-pkgId-:Mod:Contract,ValueRecord0(Some(-pkgId-:Mod:Contract),ImmArray((Some(key),ValueRecord0(Some(-pkgId-:Mod:Key),ImmArray((Some(party),ValueParty(Alice)),(Some(idx),ValueInt64(0))))),(Some(cids),ValueList(FrontStack())))),,None,Set(Alice),Set(Alice),Some(KeyWithMaintainers(ValueRecord0(Some(-pkgId-:Mod:Key),ImmArray((Some(party),ValueParty(Alice)),(Some(idx),ValueInt64(0)))),Set(Alice))),VDev), NodeId(1) -> NodeFetch(ContractId(#01),-pkgId-:Mod:Contract,None,Set(Alice),Set(Alice),Set(Alice),Some(KeyWithMaintainers(ValueRecord0(Some(-pkgId-:Mod:Key),ImmArray((Some(party),ValueParty(Alice)),(Some(idx),ValueInt64(0)))),Set(Alice))),true,VDev)),ImmArray(NodeId(0),NodeId(1))))

    }
  }

}
