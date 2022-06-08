// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{TransactionMeta, Update}
import com.daml.lf.crypto
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.CommittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.platform.index.InMemoryStateUpdaterSpec.{
  offset,
  txLogUpdate1,
  txLogUpdate3,
  update1,
  update2,
  update3,
}
import com.daml.platform.indexer.ha.EndlessReadService.configuration
import com.daml.platform.store.interfaces.TransactionLogUpdate
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.chaining._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

class InMemoryStateUpdaterSpec extends AsyncFlatSpec with Matchers with AkkaBeforeAndAfterAll {
  behavior of classOf[InMemoryStateUpdater].getSimpleName

  "flow" should "correctly process updates" in withFixture {
    case (inMemoryStateUpdater, cacheUpdates, ledgerEndUpdates) =>
      val updatesInput = Seq(Vector(update1, update2) -> 1L, Vector(update3) -> 3L)

      Source(updatesInput)
        .via(inMemoryStateUpdater.flow)
        .runWith(Sink.ignore)
        .map { _ =>
          cacheUpdates should contain theSameElementsInOrderAs Seq(
            Vector(txLogUpdate1),
            Vector(txLogUpdate3),
          )
          ledgerEndUpdates should contain theSameElementsInOrderAs Seq(
            offset(2L) -> 1L,
            offset(3L) -> 3L,
          )
        }
  }

  "flow" should "not process empty bacthes" in withFixture {
    case (inMemoryStateUpdater, cacheUpdates, ledgerEndUpdates) =>
      val updatesInput = Seq(Vector.empty -> 1L, Vector(update3) -> 3L)

      Source(updatesInput)
        .via(inMemoryStateUpdater.flow)
        .runWith(Sink.ignore)
        .map { _ =>
          cacheUpdates should contain theSameElementsInOrderAs Seq(
            Vector(txLogUpdate3)
          )
          ledgerEndUpdates should contain theSameElementsInOrderAs Seq(
            offset(3L) -> 3L
          )
        }
  }

  private def withFixture(
      test: (
          (
              InMemoryStateUpdater,
              ArrayBuffer[Vector[TransactionLogUpdate]],
              ArrayBuffer[(Offset, Long)],
          )
      ) => Future[Assertion]
  ): Future[Assertion] = {
    val updateToTransactionAccepted
        : (Offset, Update.TransactionAccepted) => TransactionLogUpdate = {
      case `update1` => txLogUpdate1
      case `update3` => txLogUpdate3
      case _ => fail()
    }

    val cacheUpdates = ArrayBuffer.empty[Vector[TransactionLogUpdate]]
    val cachesUpdateCaptor =
      (v: Vector[TransactionLogUpdate]) => cacheUpdates.addOne(v).pipe(_ => ())

    val ledgerEndUpdates = ArrayBuffer.empty[(Offset, Long)]

    val inMemoryStateUpdater = new InMemoryStateUpdater(
      2,
      scala.concurrent.ExecutionContext.global,
      scala.concurrent.ExecutionContext.global,
    )(
      updateToTransactionAccepted = updateToTransactionAccepted,
      updateCaches = cachesUpdateCaptor,
      updateLedgerEnd = { case (offset, evtSeqId) =>
        ledgerEndUpdates.addOne(offset -> evtSeqId)
      },
    )
    test(inMemoryStateUpdater, cacheUpdates, ledgerEndUpdates)
  }
}

object InMemoryStateUpdaterSpec {
  private val participantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("EndlessReadServiceParticipant")

  private val txId1 = Ref.TransactionId.assertFromString("tx1")
  private val txId2 = Ref.TransactionId.assertFromString("tx2")

  private val someSubmissionId: Ref.SubmissionId =
    Ref.SubmissionId.assertFromString("some submission id")
  private val workflowId: Ref.WorkflowId = Ref.WorkflowId.assertFromString("Workflow")
  private val someTransactionMeta: TransactionMeta = TransactionMeta(
    ledgerEffectiveTime = Timestamp.Epoch,
    workflowId = Some(workflowId),
    submissionTime = Timestamp.Epoch,
    submissionSeed = crypto.Hash.hashPrivateKey("SomeTxMeta"),
    optUsedPackages = None,
    optNodeSeeds = None,
    optByKeyNodes = None,
  )

  private val update1 = offset(1L) -> Update.TransactionAccepted(
    optCompletionInfo = None,
    transactionMeta = someTransactionMeta,
    transaction = CommittedTransaction(TransactionBuilder.Empty),
    transactionId = txId1,
    recordTime = Timestamp.Epoch,
    divulgedContracts = List.empty,
    blindingInfo = None,
  )
  private val update2 = offset(2L) -> Update.ConfigurationChanged(
    Timestamp.Epoch,
    someSubmissionId,
    participantId,
    configuration,
  )
  private val update3 = offset(3L) -> Update.TransactionAccepted(
    optCompletionInfo = None,
    transactionMeta = someTransactionMeta,
    transaction = CommittedTransaction(TransactionBuilder.Empty),
    transactionId = txId2,
    recordTime = Timestamp.Epoch,
    divulgedContracts = List.empty,
    blindingInfo = None,
  )

  private val txLogUpdate1 = TransactionLogUpdate.Transaction(
    transactionId = "tx1",
    workflowId = "",
    effectiveAt = Timestamp.Epoch,
    offset = offset(1L),
    events = Vector(null),
  )

  private val txLogUpdate3 = TransactionLogUpdate.Transaction(
    transactionId = "tx3",
    workflowId = "",
    effectiveAt = Timestamp.Epoch,
    offset = offset(3L),
    events = Vector(null),
  )

  private def offset(idx: Long): Offset = {
    val base = BigInt(1) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }
}
