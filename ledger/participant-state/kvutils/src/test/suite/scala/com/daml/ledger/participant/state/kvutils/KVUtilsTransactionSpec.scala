// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.codahale.metrics
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlTransactionRejectionEntry
}
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.v1.Update
import com.digitalasset.daml.lf.command.{
  Command,
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseCommand
}
import com.digitalasset.daml.lf.data.{Ref, FrontStack, SortedLookupList}
import com.digitalasset.daml.lf.transaction.Node.NodeCreate
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ValueUnit,
  ValueParty,
  ValueOptional,
  ValueList,
  ValueVariant,
  ValueRecord,
  ValueTextMap
}
import org.scalatest.{Matchers, WordSpec}

class KVUtilsTransactionSpec extends WordSpec with Matchers {

  import KVTest._

  "transaction" should {

    val alice = party("Alice")
    val bob = party("Bob")
    val eve = party("Eve")
    val bobValue = ValueParty(bob)

    val templateArgs: Map[String, Value[AbsoluteContractId]] = Map(
      "Party" -> bobValue,
      "Option Party" -> ValueOptional(Some(bobValue)),
      "List Party" -> ValueList(FrontStack(bobValue)),
      "TextMap Party" -> ValueTextMap(SortedLookupList(Map("bob" -> bobValue))),
      "Simple:SimpleVariant" ->
        ValueVariant(Some(typeConstructorId("Simple:SimpleVariant")), name("SV"), bobValue),
      "DA.Types:Tuple2 Party Unit" ->
        ValueRecord(
          Some(typeConstructorId("DA.Types:Tuple2 Party Unit", "DA.Types:Tuple2")),
          FrontStack(
            Some(name("x1")) -> bobValue,
            Some(name("x2")) -> ValueUnit
          ).toImmArray
        ),
      // Not yet supported in DAML:
      //
      // "<party: Party>" -> Value.ValueStruct(FrontStack(Ref.Name.assertFromString("party") -> bobValue).toImmArray),
      // "GenMap Party Unit" -> Value.ValueGenMap(FrontStack(bobValue -> ValueUnit).toImmArray),
      // "GenMap Unit Party" -> Value.ValueGenMap(FrontStack(ValueUnit -> bobValue).toImmArray),
    )

    def createCmd(
        templateId: Ref.Identifier,
        templateArg: Value[Value.AbsoluteContractId]): Command =
      CreateCommand(templateId, templateArg)

    val simpleTemplateId = templateIdWith("Party")
    val simpleTemplateArg = mkTemplateArg("Alice", "Eve", "Party", bobValue)
    val simpleCreateCmd = createCmd(simpleTemplateId, simpleTemplateArg)

    def exerciseCmd(coid: String, templateId: Ref.Identifier): Command =
      ExerciseCommand(
        templateId,
        Ref.ContractIdString.assertFromString(coid),
        simpleConsumeChoiceid,
        ValueUnit)

    def createAndExerciseCmd(
        templateId: Ref.Identifier,
        templateArg: Value[Value.AbsoluteContractId]): Command =
      CreateAndExerciseCommand(templateId, templateArg, simpleConsumeChoiceid, ValueUnit)

    val p0 = mkParticipantId(0)
    val p1 = mkParticipantId(1)

    "be able to submit transaction" in KVTest.runTestWithSimplePackage(alice, bob, eve)(
      for {
        tx <- runSimpleCommand(alice, simpleCreateCmd)
        logEntry <- submitTransaction(submitter = alice, tx = tx).map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
      }
    )

    /* Disabled while we rework the time model.
    "reject transaction with elapsed max record time" in KVTest.runTestWithSimplePackage(
      for {
        tx <- runSimpleCommand(alice, simpleCreateCmd)
        logEntry <- submitTransaction(submitter = alice, tx = tx, mrtDelta = Duration.ZERO)
          .map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        logEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.MAXIMUM_RECORD_TIME_EXCEEDED
      }
    )
     */

    "reject transaction with out of bounds LET" in KVTest.runTestWithSimplePackage(alice, bob, eve)(
      for {
        tx <- runSimpleCommand(alice, simpleCreateCmd)
        conf <- getDefaultConfiguration
        logEntry <- submitTransaction(submitter = alice, tx = tx, letDelta = conf.timeModel.minTtl)
          .map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        // FIXME(JM): Bad reason, need one for bad/expired LET!
        logEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.MAXIMUM_RECORD_TIME_EXCEEDED
      }
    )

    "be able to exercise and rejects double spends" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve) {
      for {
        createTx <- runSimpleCommand(alice, simpleCreateCmd)
        result <- submitTransaction(submitter = alice, tx = createTx)
        (entryId, logEntry) = result
        update = KeyValueConsumption.logEntryToUpdate(entryId, logEntry).head
        coid = update
          .asInstanceOf[Update.TransactionAccepted]
          .transaction
          .nodes
          .values
          .head
          .asInstanceOf[NodeCreate[AbsoluteContractId, _]]
          .coid

        exeTx <- runSimpleCommand(alice, exerciseCmd(coid.coid, simpleTemplateId))
        logEntry2 <- submitTransaction(submitter = alice, tx = exeTx).map(_._2)

        // Try to double consume.
        logEntry3 <- submitTransaction(submitter = alice, tx = exeTx).map(_._2)

      } yield {
        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
        logEntry3.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        logEntry3.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.DISPUTED
      }
    }

    "reject transactions by unallocated submitters" in KVTest.runTestWithSimplePackage(bob, eve) {
      for {
        configEntry <- submitConfig { c =>
          c.copy(generation = c.generation + 1)
        }
        createTx <- runSimpleCommand(alice, simpleCreateCmd)
        txEntry <- submitTransaction(submitter = alice, tx = createTx).map(_._2)
      } yield {
        configEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
        txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        txEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.PARTY_NOT_KNOWN_ON_LEDGER
      }
    }

    for ((additionalContractDataType, additionalContractValue) <- templateArgs) {
      val command = createCmd(
        templateIdWith(additionalContractDataType),
        mkTemplateArg("Alice", "Eve", additionalContractDataType, additionalContractValue))
      s"accept transactions with unallocated parties in values: $additionalContractDataType" in KVTest
        .runTestWithPackage(additionalContractDataType, alice, eve) {

          for {
            createTx <- runCommand(alice, additionalContractDataType, command)
            txEntry <- submitTransaction(submitter = alice, tx = createTx).map(_._2)
          } yield {
            txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
          }
        }
    }

    "reject transactions with unallocated informee" in KVTest.runTestWithSimplePackage(alice, bob) {
      for {
        createTx <- runSimpleCommand(alice, simpleCreateCmd)
        txEntry1 <- submitTransaction(submitter = alice, tx = createTx).map(_._2)
      } yield {
        txEntry1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        txEntry1.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.PARTY_NOT_KNOWN_ON_LEDGER
      }
    }

    "reject transactions for unhosted parties" in KVTest.runTestWithSimplePackage(bob, eve) {
      for {
        configEntry <- submitConfig { c =>
          c.copy(generation = c.generation + 1)
        }
        createTx <- runSimpleCommand(alice, simpleCreateCmd)

        newParty <- withParticipantId(p1)(allocateParty("unhosted", alice))
        txEntry1 <- withParticipantId(p0)(
          submitTransaction(submitter = newParty, tx = createTx).map(_._2))
        txEntry2 <- withParticipantId(p1)(
          submitTransaction(submitter = newParty, tx = createTx).map(_._2))

      } yield {
        configEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
        txEntry1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        txEntry1.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT
        txEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY

      }
    }

    "reject unauthorized transactions" in KVTest.runTestWithSimplePackage(alice, eve) {
      for {
        // Submit a creation of a contract with owner 'Alice', but submit it as 'Bob'.
        createTx <- runSimpleCommand(alice, simpleCreateCmd)
        bob <- allocateParty("bob", bob)
        txEntry <- submitTransaction(submitter = bob, tx = createTx).map(_._2)
      } yield {
        txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        val disputed = DamlTransactionRejectionEntry.ReasonCase.DISPUTED
        txEntry.getTransactionRejectionEntry.getReasonCase shouldEqual disputed
      }
    }

    "metrics get updated" in KVTest.runTestWithSimplePackage(alice, eve) {
      for {
        // Submit a creation of a contract with owner 'Alice', but submit it as 'Bob'.
        createTx <- runSimpleCommand(alice, simpleCreateCmd)
        bob <- allocateParty("bob", bob)
        _ <- submitTransaction(submitter = bob, tx = createTx).map(_._2)
      } yield {
        val disputed = DamlTransactionRejectionEntry.ReasonCase.DISPUTED
        // Check that we're updating the metrics (assuming this test at least has been run)
        val reg = metrics.SharedMetricRegistries.getOrCreate("kvutils")
        reg.counter("kvutils.committer.transaction.accepts").getCount should be >= 1L
        reg
          .counter(s"kvutils.committer.transaction.rejections_${disputed.name}")
          .getCount should be >= 1L
        reg.timer("kvutils.committer.transaction.run_timer").getCount should be >= 1L
      }
    }

    "transient contracts and keys are properly archived" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve) {
      val simpleCreateAndExerciseCmd = createAndExerciseCmd(simpleTemplateId, simpleTemplateArg)

      for {
        tx1 <- runSimpleCommand(alice, simpleCreateAndExerciseCmd)
        createAndExerciseTx1 <- submitTransaction(alice, tx1).map(_._2)

        tx2 <- runSimpleCommand(alice, simpleCreateAndExerciseCmd)
        createAndExerciseTx2 <- submitTransaction(alice, tx2).map(_._2)

        finalState <- scalaz.State.get[KVTestState]
      } yield {
        createAndExerciseTx1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
        createAndExerciseTx2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY

        // Check that all contracts and keys are in the archived state.
        finalState.damlState.foreach {
          case (_, v) =>
            v.getValueCase match {
              case DamlKvutils.DamlStateValue.ValueCase.CONTRACT_KEY_STATE =>
                val cks = v.getContractKeyState
                cks.hasContractId shouldBe false

              case DamlKvutils.DamlStateValue.ValueCase.CONTRACT_STATE =>
                val cs = v.getContractState
                cs.hasArchivedAt shouldBe true

              case _ =>
                succeed
            }
        }
      }
    }

    "submitter info is optional" in KVTest.runTestWithSimplePackage(alice, bob, eve) {
      for {
        createTx <- runSimpleCommand(alice, simpleCreateCmd)
        result <- submitTransaction(submitter = alice, tx = createTx)
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
}
