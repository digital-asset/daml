// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlTransactionRejectionEntry
}
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.v1.Update
import com.daml.lf.command.Command
import com.daml.lf.crypto
import com.daml.lf.data.{FrontStack, SortedLookupList}
import com.daml.lf.transaction.Node.NodeCreate
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{
  ContractId,
  ValueList,
  ValueOptional,
  ValueParty,
  ValueRecord,
  ValueTextMap,
  ValueUnit,
  ValueVariant
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KVUtilsTransactionSpec extends AnyWordSpec with Matchers {

  import KVTest._

  "transaction" should {

    val alice = party("Alice")
    val bob = party("Bob")
    val eve = party("Eve")
    val bobValue = ValueParty(bob)

    def simpleCreateCmd(simplePackage: SimplePackage): Command =
      simplePackage.simpleCreateCmd(mkSimpleCreateArg(simplePackage))

    def mkSimpleCreateArg(simplePackage: SimplePackage): Value[ContractId] =
      simplePackage.mkSimpleTemplateArg("Alice", "Eve", bobValue)

    val templateArgs: Map[String, SimplePackage => Value[ContractId]] = Map(
      "Party" -> (_ => bobValue),
      "Option Party" -> (_ => ValueOptional(Some(bobValue))),
      "List Party" -> (_ => ValueList(FrontStack(bobValue))),
      "TextMap Party" -> (_ => ValueTextMap(SortedLookupList(Map("bob" -> bobValue)))),
      "Simple:SimpleVariant" -> (
          simplePackage =>
            ValueVariant(
              Some(simplePackage.typeConstructorId("Simple:SimpleVariant")),
              name("SV"),
              bobValue,
            )),
      "DA.Types:Tuple2 Party Unit" -> (
          simplePackage =>
            ValueRecord(
              Some(simplePackage.typeConstructorId("DA.Types:Tuple2")),
              FrontStack(
                Some(name("x1")) -> bobValue,
                Some(name("x2")) -> ValueUnit
              ).toImmArray,
            )),
      // Not yet supported in DAML:
      //
      // "<party: Party>" -> Value.ValueStruct(FrontStack(Ref.Name.assertFromString("party") -> bobValue).toImmArray),
      // "GenMap Party Unit" -> Value.ValueGenMap(FrontStack(bobValue -> ValueUnit).toImmArray),
      // "GenMap Unit Party" -> Value.ValueGenMap(FrontStack(ValueUnit -> bobValue).toImmArray),
    )

    val p0 = mkParticipantId(0)
    val p1 = mkParticipantId(1)

    "be able to submit a transaction" in KVTest.runTestWithSimplePackage(alice, bob, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          logEntry <- submitTransaction(
            submitter = alice,
            transaction = transaction,
            submissionSeed = seed).map(_._2)
        } yield {
          logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
          logEntry.getTransactionEntry.hasBlindingInfo shouldBe true
        }
    }

    "be able to pre-execute a transaction" in KVTest.runTestWithSimplePackage(alice, bob, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          preExecutionResult <- preExecuteTransaction(
            submitter = alice,
            transaction = transaction,
            submissionSeed = seed,
          ).map(_._2)
        } yield {
          preExecutionResult.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
          preExecutionResult.successfulLogEntry.getTransactionEntry.hasBlindingInfo shouldBe true
        }
    }

    "reject transaction with out of bounds LET" in KVTest.runTestWithSimplePackage(alice, bob, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          conf <- getDefaultConfiguration
          logEntry <- submitTransaction(
            submitter = alice,
            transaction = transaction,
            submissionSeed = seed,
            letDelta = conf.timeModel.maxSkew.plusMillis(1))
            .map(_._2)
        } yield {
          logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          logEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.INVALID_LEDGER_TIME
        }
    }

    "be able to exercise and rejects double spends" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve) { simplePackage =>
      val seeds =
        Stream
          .from(0)
          .map(i => crypto.Hash.hashPrivateKey(this.getClass.getName + i.toString))
      for {
        transaction1 <- runSimpleCommand(alice, seeds.head, simpleCreateCmd(simplePackage))
        result <- submitTransaction(
          submitter = alice,
          transaction = transaction1,
          submissionSeed = seeds.head)
        (entryId, logEntry) = result
        update = KeyValueConsumption.logEntryToUpdate(entryId, logEntry).head
        contractId = update
          .asInstanceOf[Update.TransactionAccepted]
          .transaction
          .nodes
          .values
          .head
          .asInstanceOf[NodeCreate[ContractId, _]]
          .coid

        transaction2 <- runSimpleCommand(
          alice,
          seeds(1),
          simplePackage.simpleExerciseConsumeCmd(contractId),
        )
        logEntry2 <- submitTransaction(
          submitter = alice,
          transaction = transaction2,
          submissionSeed = seeds(1)).map(_._2)

        // Try to double consume.
        logEntry3 <- submitTransaction(
          submitter = alice,
          transaction = transaction2,
          submissionSeed = seeds(1)).map(_._2)

      } yield {
        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
        logEntry3.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        logEntry3.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.DISPUTED
      }
    }

    "reject transactions by unallocated submitters" in KVTest.runTestWithSimplePackage(bob, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          configEntry <- submitConfig { c =>
            c.copy(generation = c.generation + 1)
          }
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          txEntry <- submitTransaction(
            submitter = alice,
            transaction = transaction,
            submissionSeed = seed).map(_._2)
        } yield {
          configEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
          txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          txEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.PARTY_NOT_KNOWN_ON_LEDGER
        }
    }

    for ((additionalContractDataType, additionalContractValue) <- templateArgs) {
      s"accept transactions with unallocated parties in values: $additionalContractDataType" in {
        val simplePackage = new SimplePackage(additionalContractDataType)
        val command = simplePackage.simpleCreateCmd(
          simplePackage.mkSimpleTemplateArg("Alice", "Eve", additionalContractValue(simplePackage)))
        KVTest.runTestWithPackage(simplePackage, alice, eve) {
          val seed = hash(this.getClass.getName)
          for {
            transaction <- runCommand(alice, seed, command)
            txEntry <- submitTransaction(
              submitter = alice,
              transaction = transaction,
              submissionSeed = seed,
            ).map(_._2)
          } yield {
            txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
          }
        }
      }
    }

    "reject transactions with unallocated informee" in KVTest.runTestWithSimplePackage(alice, bob) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          txEntry1 <- submitTransaction(
            submitter = alice,
            transaction = transaction,
            submissionSeed = seed)
            .map(_._2)
        } yield {
          txEntry1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          txEntry1.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.PARTY_NOT_KNOWN_ON_LEDGER
        }
    }

    // FIXME: review this test
    "reject transactions for unhosted parties" in KVTest.runTestWithSimplePackage(bob, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          configEntry <- submitConfig { c =>
            c.copy(generation = c.generation + 1)
          }
          createTx <- withParticipantId(p1)(
            runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage)))

          newParty <- withParticipantId(p1)(allocateParty("unhosted", alice))
          txEntry1 <- withParticipantId(p0)(
            submitTransaction(submitter = newParty, createTx, seed).map(_._2))
          txEntry2 <- withParticipantId(p1)(
            submitTransaction(submitter = newParty, transaction = createTx, submissionSeed = seed)
              .map(_._2))

        } yield {
          configEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
          txEntry1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          txEntry1.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT
          txEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY

        }
    }

    "reject unauthorized transactions" in KVTest.runTestWithSimplePackage(alice, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          // Submit a creation of a contract with owner 'Alice', but submit it as 'Bob'.
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          bob <- allocateParty("bob", bob)
          txEntry <- submitTransaction(
            submitter = bob,
            transaction = transaction,
            submissionSeed = seed)
            .map(_._2)
        } yield {
          txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          val disputed = DamlTransactionRejectionEntry.ReasonCase.DISPUTED
          txEntry.getTransactionRejectionEntry.getReasonCase shouldEqual disputed
        }
    }

    "update metrics" in KVTest.runTestWithSimplePackage(alice, eve) { simplePackage =>
      val seed = hash(this.getClass.getName)
      for {
        // Submit a creation of a contract with owner 'Alice', but submit it as 'Bob'.
        transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
        bob <- allocateParty("bob", bob)
        _ <- submitTransaction(submitter = bob, transaction = transaction, submissionSeed = seed)
          .map(_._2)
      } yield {
        val disputed = DamlTransactionRejectionEntry.ReasonCase.DISPUTED
        // Check that we're updating the metrics (assuming this test at least has been run)
        metrics.daml.kvutils.committer.transaction.accepts.getCount should be >= 1L
        metrics.daml.kvutils.committer.transaction.rejection(disputed.name).getCount should be >= 1L
        metrics.daml.kvutils.committer.runTimer("transaction").getCount should be >= 1L
        metrics.daml.kvutils.committer.transaction.interpretTimer.getCount should be >= 1L
        metrics.daml.kvutils.committer.transaction.runTimer.getCount should be >= 1L
      }
    }

    "properly archive transient contracts and keys" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve) { simplePackage =>
      val seeds =
        Stream
          .from(0)
          .map(i => crypto.Hash.hashPrivateKey(this.getClass.getName + i.toString))

      val simpleCreateAndExerciseCmd =
        simplePackage.simpleCreateAndExerciseConsumeCmd(mkSimpleCreateArg(simplePackage))

      for {
        tx1 <- runSimpleCommand(alice, seeds.head, simpleCreateAndExerciseCmd)
        createAndExerciseTx1 <- submitTransaction(alice, tx1, seeds.head).map(_._2)

        tx2 <- runSimpleCommand(alice, seeds(1), simpleCreateAndExerciseCmd)
        createAndExerciseTx2 <- submitTransaction(alice, tx2, seeds(1)).map(_._2)

        finalState <- scalaz.State.get[KVTestState]
      } yield {
        createAndExerciseTx1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
        createAndExerciseTx2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY

        // Check that all contracts and keys are in the archived state.
        finalState.damlState.foreach {
          case (_, v) =>
            v.getValueCase match {
              case DamlKvutils.DamlStateValue.ValueCase.CONTRACT_KEY_STATE =>
                v.getContractKeyState.getContractId shouldBe 'empty

              case DamlKvutils.DamlStateValue.ValueCase.CONTRACT_STATE =>
                val cs = v.getContractState
                cs.hasArchivedAt shouldBe true

              case _ =>
                succeed
            }
        }
      }
    }

    "allow for missing submitter info in log entries" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve) { simplePackage =>
      val seed = hash(this.getClass.getName)
      for {
        transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
        result <- submitTransaction(
          submitter = alice,
          transaction = transaction,
          submissionSeed = seed,
        )
      } yield {
        val (entryId, entry) = result
        // Clear the submitter info from the log entry
        val strippedEntry = entry.toBuilder
        strippedEntry.getTransactionEntryBuilder.clearSubmitterInfo

        // Process into updates and verify
        val updates = KeyValueConsumption.logEntryToUpdate(entryId, strippedEntry.build)
        updates should have length 1
        updates.head should be(a[Update.TransactionAccepted])
        val txAccepted = updates.head.asInstanceOf[Update.TransactionAccepted]
        txAccepted.optSubmitterInfo should be(None)
      }
    }
  }

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)
}
