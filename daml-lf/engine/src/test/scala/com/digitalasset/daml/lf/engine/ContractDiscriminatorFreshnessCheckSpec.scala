// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.data._
import com.daml.lf.engine.Engine
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.Node
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.lf.value.{Value, ValueVersions}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.language.implicitConversions

class ContractDiscriminatorFreshnessCheckSpec
    extends WordSpec
    with Matchers
    with TableDrivenPropertyChecks {

  private val pkg = p"""
         module Mod {
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
                choice @nonConsuming Noop (self) (u: Unit) : Unit
                  by 
                    Mod:contractParties this
                  to
                    upure @Unit (),
                 choice @nonConsuming LookupByKey (self) (key: Mod:Key) : Option (ContractId Mod:Contract)
                   by 
                     Mod:contractParties this
                   to
                     lookup_by_key @Mod:Contract key 
                     
              },
              key @Mod:Key (Mod:Contract {key} this) Mod:keyParties
            };
         }
         """
  private val pkgId = defaultParserParameters.defaultPackageId
  private val pkgs = Map(pkgId -> pkg).lift

  private val keyId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Key"))
  private val tmplId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Contract"))

  private def contractId(discriminator: crypto.Hash, suffix: Bytes): Value.AbsoluteContractId =
    Value.AbsoluteContractId.V1.assertBuild(discriminator, suffix)

  private def keyRecord(party: Ref.Party, idx: Int) =
    Value.ValueRecord(
      Some(keyId),
      ImmArray(
        Some[Ref.Name]("party") -> Value.ValueParty(party),
        Some[Ref.Name]("idx") -> Value.ValueInt64(idx.toLong),
      )
    )

  private def contractRecord(party: Ref.Party, idx: Int, cids: List[AbsoluteContractId]) =
    Value.ValueRecord(
      Some(tmplId),
      ImmArray(
        Some[Ref.Name]("key") -> keyRecord(party, idx),
        Some[Ref.Name]("cids") -> Value.ValueList(FrontStack(cids.map(Value.ValueContractId(_))))
      )
    )

  private val hash1 = crypto.Hash.hashPrivateKey("hash1")
  private val hash2 = crypto.Hash.hashPrivateKey("hash2")
  private val suffix1 = Utf8.getBytes("suffix")
  private val suffix2 = Utf8.getBytes("extension")
  private val suffix3 = Utf8.getBytes("final-addition")

  private def contractInstance(party: Ref.Party, idx: Int, cids: List[AbsoluteContractId]) =
    Value.ContractInst(
      tmplId,
      ValueVersions.assertAsVersionedValue(contractRecord(party, idx, cids)),
      "Agreement",
    )

  private def globalKey(party: Ref.Party, idx: Int) =
    Node.GlobalKey(tmplId, keyRecord(party, idx))

  private val alice = Ref.Party.assertFromString("Alice")
  private val participant = Ref.ParticipantId.assertFromString("participant")

  private val submissionSeed = crypto.Hash.hashPrivateKey(getClass.getName)
  private val let = Time.Timestamp.MinValue
  private val transactionSeed = crypto.Hash.deriveTransactionSeed(
    submissionSeed,
    participant,
    let
  )

  private def submit(
      cmds: ImmArray[command.Command],
      pcs: Value.AbsoluteContractId => Option[
        Value.ContractInst[Value.VersionedValue[AbsoluteContractId]]],
      keys: Node.GlobalKey => Option[AbsoluteContractId]
  ) =
    engine
      .submit(
        cmds = command.Commands(
          submitter = alice,
          commands = cmds,
          ledgerEffectiveTime = let,
          commandsReference = "test",
        ),
        participantId = participant,
        submissionSeed = submissionSeed,
      )
      .consume(pcs, pkgs, keys)

  val engine = new Engine()

  "conflict freshness check" should {

    "fails when a global contract conflicts with a local contract previously created" ignore {

      val conflictingCid = {
        val createNodeSeed = crypto.Hash.deriveNodeSeed(transactionSeed, 0)
        val conflictingDiscriminator =
          crypto.Hash.deriveContractDiscriminator(createNodeSeed, let, Set(alice))
        contractId(conflictingDiscriminator, suffix3)
      }

      val exercisedCid1 = contractId(hash1, suffix1)
      val exercisedCid2 = contractId(hash2, suffix2)
      val contractsData =
        List(
          (exercisedCid1, 1, List.empty),
          (exercisedCid2, 2, List(conflictingCid)),
          (conflictingCid, 3, List.empty)
        )
      val contractLookup =
        contractsData
          .map {
            case (cid, idx, cids) => cid -> contractInstance(alice, idx, cids)
          }
          .toMap
          .lift
      val keyLookup = contractsData
        .map {
          case (cid, idx, _) => globalKey(alice, idx) -> cid
        }
        .toMap
        .lift

      def run(cmd: command.Command) =
        submit(
          ImmArray(
            command.CreateCommand(tmplId, contractRecord(alice, 0, List.empty)),
            cmd,
          ),
          pcs = contractLookup,
          keys = keyLookup,
        )

      val negativeTestCases = Table(
        "successful commands",
        command.ExerciseCommand(tmplId, exercisedCid1, "Noop", Value.ValueUnit),
        command.ExerciseByKeyCommand(tmplId, keyRecord(alice, 1), "Noop", Value.ValueUnit),
        command.ExerciseCommand(tmplId, exercisedCid1, "LookupByKey", keyRecord(alice, -1)),
        command.ExerciseCommand(tmplId, exercisedCid1, "LookupByKey", keyRecord(alice, 1)),
        command.ExerciseCommand(tmplId, exercisedCid1, "LookupByKey", keyRecord(alice, 2)),
      )

      val positiveTestCases = Table(
        "failing commands",
        command.ExerciseCommand(tmplId, conflictingCid, "Noop", Value.ValueUnit),
        command.ExerciseCommand(tmplId, exercisedCid2, "Noop", Value.ValueUnit),
        command.ExerciseByKeyCommand(tmplId, keyRecord(alice, 2), "Noop", Value.ValueUnit),
        command.ExerciseByKeyCommand(tmplId, keyRecord(alice, 3), "Noop", Value.ValueUnit),
        command.ExerciseCommand(tmplId, exercisedCid1, "LookupByKey", keyRecord(alice, 3)),
      )

      forAll(negativeTestCases) { cmd =>
        run(cmd) shouldBe 'right
      }

      forAll(positiveTestCases) { cmd =>
        val r = run(cmd)
        r shouldBe 'left
        r.left.exists(_.msg.contains("Conflicting discriminators")) shouldBe true
      }

    }

    "fails when  a local conflicts with a local contract previously fetched" in {

      val conflictingCid = {
        val createNodeSeed = crypto.Hash.deriveNodeSeed(transactionSeed, 1)
        val conflictingDiscriminator =
          crypto.Hash.deriveContractDiscriminator(createNodeSeed, let, Set(alice))
        contractId(conflictingDiscriminator, suffix3)
      }

      val exercisedCid1 = contractId(hash1, suffix1)
      val exercisedCid2 = contractId(hash2, suffix2)
      val contractsData =
        List(
          (exercisedCid1, 1, List.empty),
          (exercisedCid2, 2, List(conflictingCid)),
          (conflictingCid, 3, List.empty)
        )
      val contractLookup =
        contractsData
          .map {
            case (cid, idx, cids) => cid -> contractInstance(alice, idx, cids)
          }
          .toMap
          .lift
      val keyLookup = contractsData
        .map {
          case (cid, idx, _) => globalKey(alice, idx) -> cid
        }
        .toMap
        .lift

      def run(cmd: command.Command) =
        submit(
          ImmArray(
            cmd,
            command.CreateCommand(tmplId, contractRecord(alice, 0, List.empty)),
          ),
          pcs = contractLookup,
          keys = keyLookup,
        )

      val negativeTestCases = Table(
        "successful commands",
        command.ExerciseCommand(tmplId, exercisedCid1, "Noop", Value.ValueUnit),
        command.ExerciseByKeyCommand(tmplId, keyRecord(alice, 1), "Noop", Value.ValueUnit),
        command.ExerciseCommand(tmplId, exercisedCid1, "LookupByKey", keyRecord(alice, -1)),
        command.ExerciseCommand(tmplId, exercisedCid1, "LookupByKey", keyRecord(alice, 1)),
        command.ExerciseCommand(tmplId, exercisedCid1, "LookupByKey", keyRecord(alice, 2)),
      )

      val positiveTestCases = Table(
        "failing commands",
        command.ExerciseCommand(tmplId, conflictingCid, "Noop", Value.ValueUnit),
        command.ExerciseCommand(tmplId, exercisedCid2, "Noop", Value.ValueUnit),
        command.ExerciseByKeyCommand(tmplId, keyRecord(alice, 2), "Noop", Value.ValueUnit),
        command.ExerciseByKeyCommand(tmplId, keyRecord(alice, 3), "Noop", Value.ValueUnit),
        command.ExerciseCommand(tmplId, exercisedCid1, "LookupByKey", keyRecord(alice, 3)),
      )

      forAll(negativeTestCases) { cmd =>
        Right(cmd) shouldBe 'right
      }

      forAll(positiveTestCases) { cmd =>
        val r = run(cmd)
        r shouldBe 'left
        r.left.exists(_.msg.contains("Conflicting discriminators")) shouldBe true
      }

    }

  }

  private implicit def toName(s: String): Ref.Name = Ref.Name.assertFromString(s)

}
