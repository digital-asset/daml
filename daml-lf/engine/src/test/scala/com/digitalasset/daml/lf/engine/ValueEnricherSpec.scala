// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.crypto.Hash
import com.daml.lf.data._
import com.daml.lf.language.Ast.{TNat, TTyCon, Type}
import com.daml.lf.language.Util._
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.lf.value.Value._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValueEnricherSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import TransactionBuilder.Implicits.{defaultPackageId => _, _}
  import com.daml.lf.testing.parser.Implicits._

  private[this] implicit val defaultPackageId: Ref.PackageId =
    defaultParserParameters.defaultPackageId

  private def cid(key: String): ContractId = ContractId.V1(Hash.hashPrivateKey(key))

  val pkg =
    p"""
        module Mod {

          record @serializable MyUnit = {};
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

          val keyParties: (Mod:Key -> List Party) =
            \(key: Mod:Key) ->
              Cons @Party [Mod:Key {party} key] (Nil @Party);

          val contractParties : (Mod:Contract -> List Party) =
            \(contract: Mod:Contract) ->
              Mod:keyParties (Mod:Contract {key} contract);

          record @serializable View = {
            signatory: List Party,
            cids: List (ContractId Mod:Contract)
          };

          interface (this: I) = {
            viewtype Mod:View;
          };

          template (this : Contract) =  {
             precondition True;
             signatories Mod:contractParties this;
             observers Mod:contractParties this;
             agreement "Agreement";
             choice @nonConsuming Noop (self) (r: Mod:Record) : Mod:Record,
               controllers
                 Mod:contractParties this
               to
                 upure @Mod:Record r;
             implements Mod:I {
               view = Mod:View { signatory = Mod:contractParties this, cids = Mod:Contract {cids} this } ;
             };
             key @Mod:Key (Mod:Contract {key} this) Mod:keyParties;
          };
        }

    """

  private[this] val engine = Engine.DevEngine()

  engine
    .preloadPackage(defaultPackageId, pkg)
    .consume(_ => None, _ => None, _ => None)
    .left
    .foreach(err => sys.error(err.message))

  private[this] val enricher = new ValueEnricher(engine)

  "enrichValue" should {

    val testCases = Table[Type, Value, Value](
      ("type", "input", "expected output"),
      (TUnit, ValueUnit, ValueUnit),
      (TBool, ValueTrue, ValueTrue),
      (TInt64, ValueInt64(42), ValueInt64(42)),
      (TTimestamp, ValueTimestamp("1969-07-20T20:17:00Z"), ValueTimestamp("1969-07-20T20:17:00Z")),
      (TDate, ValueDate("1879-03-14"), ValueDate("1879-03-14")),
      (TText, ValueText("daml"), ValueText("daml")),
      (
        TNumeric(TNat(Decimal.scale)),
        ValueNumeric("10."),
        ValueNumeric("10.0000000000"),
      ),
      (TParty, ValueParty("Alice"), ValueParty("Alice")),
      (
        TContractId(TTyCon("Mod:Record")),
        ValueContractId(cid("#contractId").coid),
        ValueContractId(cid("#contractId").coid),
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
        TTyCon("Mod:Record"),
        ValueRecord(None, ImmArray(None -> ValueInt64(33))),
        ValueRecord(Some("Mod:Record"), ImmArray(Some[Ref.Name]("field") -> ValueInt64(33))),
      ),
      (
        TTyCon("Mod:Variant"),
        ValueVariant(None, "variant1", ValueText("some test")),
        ValueVariant(Some("Mod:Variant"), "variant1", ValueText("some test")),
      ),
      (TTyCon("Mod:Enum"), ValueEnum(None, "value1"), ValueEnum(Some("Mod:Enum"), "value1")),
    )

    "enrich values as expected" in {
      forEvery(testCases) { (typ, input, output) =>
        enricher.enrichValue(typ, input) shouldBe ResultDone(output)
      }
    }
  }

  "enrichValue" should {
    val alice = Ref.Party.assertFromString("alice")
    val view = Value.ValueRecord(
      None,
      ImmArray(
        None -> ValueList(FrontStack(ValueParty(alice))),
        None -> ValueList(FrontStack(ValueContractId(cid("#contractId").coid))),
      ),
    )

    val enrichedView = Value.ValueRecord(
      Some("Mod:View": Ref.Identifier),
      ImmArray(
        Some("signatory": Ref.Name) -> ValueList(FrontStack(ValueParty(alice))),
        Some("cids": Ref.Name) -> ValueList(FrontStack(ValueContractId(cid("#contractId").coid))),
      ),
    )

    "enrich views as expected" in {
      enricher.enrichView("Mod:I", view) shouldBe ResultDone(enrichedView)
    }
  }

  "enrichTransaction" should {

    def buildTransaction(
        contract: Value,
        key: Value,
        record: Value,
    ) = {
      val builder = new TransactionBuilder(_ => TransactionVersion.minTypeErasure)
      val create =
        builder.create(
          id = cid("#01"),
          templateId = "Mod:Contract",
          argument = contract,
          signatories = Set("Alice"),
          observers = Set("Alice"),
          key = Some(key),
        )
      builder.add(create)
      builder.add(builder.fetch(create))
      builder.lookupByKey(create, true)
      builder.exercise(
        contract = create,
        choice = "Noop",
        consuming = false,
        actingParties = Set("Alice"),
        argument = record,
        result = Some(record),
      )
      builder.buildCommitted()
    }

    "enrich transaction as expected" in {

      val inputKey = ValueRecord(
        "",
        ImmArray(
          "" -> ValueParty("Alice"),
          "" -> Value.ValueInt64(0),
        ),
      )

      val inputContract =
        ValueRecord(
          "",
          ImmArray(
            "" -> inputKey,
            "" -> Value.ValueNil,
          ),
        )

      val inputRecord =
        ValueRecord(None, ImmArray(None -> ValueInt64(33)))

      val inputTransaction = buildTransaction(
        inputContract,
        inputKey,
        inputRecord,
      )

      val outputKey = ValueRecord(
        "Mod:Key",
        ImmArray(
          "party" -> ValueParty("Alice"),
          "idx" -> Value.ValueInt64(0),
        ),
      )

      val outputContract =
        ValueRecord(
          "Mod:Contract",
          ImmArray(
            "key" -> outputKey,
            "cids" -> Value.ValueNil,
          ),
        )

      val outputRecord =
        ValueRecord("Mod:Record", ImmArray("field" -> ValueInt64(33)))

      val outputTransaction = buildTransaction(
        outputContract,
        outputKey,
        outputRecord,
      )

      enricher.enrichVersionedTransaction(inputTransaction) shouldNot
        be(ResultDone(inputTransaction))
      enricher.enrichVersionedTransaction(inputTransaction) shouldBe ResultDone(outputTransaction)

    }
  }

}
