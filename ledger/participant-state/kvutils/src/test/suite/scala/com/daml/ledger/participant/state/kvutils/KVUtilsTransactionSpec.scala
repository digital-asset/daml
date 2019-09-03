// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlTransactionRejectionEntry
}
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.v1.Update
import com.digitalasset.daml.lf.command.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.Node.NodeCreate
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import org.scalatest.{Matchers, WordSpec}

class KVUtilsTransactionSpec extends WordSpec with Matchers {

  import KVTest._

  "transaction" should {
    val alice = Ref.Party.assertFromString("Alice")
    val bob = Ref.Party.assertFromString("Bob")

    val simpleCreateCmd: Command = CreateCommand(simpleTemplateId, mkSimpleTemplateArg("Alice"))
    def simpleExerciseCmd(coid: String): Command =
      ExerciseCommand(
        simpleTemplateId,
        Ref.ContractIdString.assertFromString(coid),
        simpleConsumeChoiceid,
        mkUnitValue)

    val p0 = mkParticipantId(0)
    val p1 = mkParticipantId(1)

    "be able to submit transaction" in KVTest.runTest(
      for {
        tx <- runCommand(alice, simpleCreateCmd)
        logEntry <- submitTransaction(submitter = alice, tx = tx).map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
      }
    )

    /* Disabled while we rework the time model.
    "reject transaction with elapsed max record time" in KVTest.runTest(
      for {
        tx <- runCommand(alice, simpleCreateCmd)
        logEntry <- submitTransaction(submitter = alice, tx = tx, mrtDelta = Duration.ZERO)
          .map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        logEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.MAXIMUM_RECORD_TIME_EXCEEDED
      }
    )
     */

    "reject transaction with out of bounds LET" in KVTest.runTest(
      for {
        tx <- runCommand(alice, simpleCreateCmd)
        conf <- getDefaultConfiguration
        logEntry <- submitTransaction(submitter = alice, tx = tx, letDelta = conf.timeModel.minTtl)
          .map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        // FIXME(JM): Bad reason, need one for bad/expired LET!
        logEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.MAXIMUM_RECORD_TIME_EXCEEDED
      }
    )

    "be able to exercise and rejects double spends" in KVTest.runTest {
      for {
        createTx <- runCommand(alice, simpleCreateCmd)
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

        exeTx <- runCommand(alice, simpleExerciseCmd(coid.coid))
        logEntry2 <- submitTransaction(submitter = alice, tx = exeTx).map(_._2)

        // Try to double consume.
        logEntry3 <- submitTransaction(submitter = alice, tx = exeTx).map(_._2)

      } yield {
        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
        logEntry3.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        logEntry3.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.DISPUTED
      }
    }

    "reject transactions for unallocated parties" in KVTest.runTest {
      for {
        configEntry <- submitConfig { c =>
          c.copy(generation = c.generation + 1, openWorld = false)
        }
        createTx <- runCommand(alice, simpleCreateCmd)
        txEntry <- submitTransaction(submitter = alice, tx = createTx).map(_._2)
      } yield {
        configEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
        txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        txEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.PARTY_NOT_KNOWN_ON_LEDGER
      }
    }

    "reject transactions for unhosted parties" in KVTest.runTest {
      for {
        configEntry <- submitConfig { c =>
          c.copy(generation = c.generation + 1, openWorld = false)
        }
        createTx <- runCommand(alice, simpleCreateCmd)

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
    "reject unauthorized transactions " in KVTest.runTest {
      for {
        // Submit a creation of a contract with owner 'Alice', but submit it as 'Bob'.
        createTx <- runCommand(alice, simpleCreateCmd)
        bob <- allocateParty("bob", bob)
        txEntry <- submitTransaction(submitter = bob, tx = createTx).map(_._2)
      } yield {
        txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        txEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.DISPUTED
      }
    }

  }
}
