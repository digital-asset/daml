// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.ledger.participant.state.v2.{CompletionInfo, TransactionMeta, Update}
import com.daml.lf.crypto
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.CommittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.metrics.Metrics
import com.daml.platform.index.InMemoryStateUpdaterSpec.{
  metadataChangedUpdate,
  offset,
  txLogUpdate1,
  txLogUpdate3,
  txRejected,
  update1,
  update3,
  update4,
}
import com.daml.lf.value.Value.ContractId
import com.daml.platform.index.InMemoryStateUpdaterSpec._
import com.daml.platform.indexer.ha.EndlessReadService.configuration
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.CompletionDetails
import com.google.rpc.status.Status
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

class InMemoryStateUpdaterSpec
    extends AsyncFlatSpec
    with Matchers
    with MockitoSugar
    with AkkaBeforeAndAfterAll {
  behavior of classOf[InMemoryStateUpdater].getSimpleName

  "flow" should "correctly process updates" in withFixture {
    case (inMemoryStateUpdater, inMemoryStateUpdates) =>
      val updatesInput =
        Seq(Vector(update1, metadataChangedUpdate) -> 1L, Vector(update3, update4) -> 3L)

      Source(updatesInput)
        .via(inMemoryStateUpdater.flow)
        .runWith(Sink.ignore)
        .map { _ =>
          inMemoryStateUpdates should contain theSameElementsInOrderAs Seq(
            (Vector(txLogUpdate1 -> contractStateEvents1), offset(2L), 1L),
            (Vector(txLogUpdate3 -> Vector.empty, txRejected -> Vector.empty), offset(4L), 3L),
          )
        }
  }

  "flow" should "not process empty input batches" in withFixture {
    case (inMemoryStateUpdater, inMemoryStateUpdates) =>
      val updatesInput =
        Seq(
          // Empty input batch should have not effect
          Vector.empty -> 1L,
          // Results in empty batch after processing
          // Should still have effect on ledger end updates
          Vector(metadataChangedUpdate) -> 3L,
          Vector(update3) -> 3L,
        )

      Source(updatesInput)
        .via(inMemoryStateUpdater.flow)
        .runWith(Sink.ignore)
        .map { _ =>
          inMemoryStateUpdates should contain theSameElementsInOrderAs Seq(
            (Vector.empty, offset(2L), 3L),
            (Vector(txLogUpdate3 -> Vector.empty), offset(3L), 3L),
          )
        }
  }

  private def withFixture(
      test: (
          (
              InMemoryStateUpdater,
              ArrayBuffer[
                (Vector[(TransactionLogUpdate, Vector[ContractStateEvent])], Offset, Long)
              ],
          )
      ) => Future[Assertion]
  ): Future[Assertion] = {
    val updateToTransactionAccepted
        : (Offset, Update.TransactionAccepted) => TransactionLogUpdate.TransactionAccepted = {
      case `update1` => txLogUpdate1
      case `update3` => txLogUpdate3
      case _ => fail()
    }

    val updateToTransactionRejected
        : (Offset, Update.CommandRejected) => TransactionLogUpdate.TransactionRejected = {
      case `update4` => txRejected
      case _ => fail()
    }

    val convertToContractStateEvents: TransactionLogUpdate => Vector[ContractStateEvent] = {
      case `txLogUpdate1` => contractStateEvents1
      case `txLogUpdate3` => Vector.empty
      case `txRejected` => Vector.empty
      case _ => fail()
    }

    val inMemoryStateUpdates =
      ArrayBuffer.empty[(Vector[(TransactionLogUpdate, Vector[ContractStateEvent])], Offset, Long)]
    val inMemoryStateUpdatesCaptor = (
        updates: Vector[(TransactionLogUpdate, Vector[ContractStateEvent])],
        lastOffset: Offset,
        lastEventSequentialId: Long,
    ) => {
      inMemoryStateUpdates.addOne((updates, lastOffset, lastEventSequentialId))
      Future.unit
    }

    val inMemoryStateUpdater = new InMemoryStateUpdater(
      2,
      scala.concurrent.ExecutionContext.global,
      scala.concurrent.ExecutionContext.global,
      new Metrics(new MetricRegistry),
    )(
      convertTransactionAccepted = updateToTransactionAccepted,
      convertTransactionRejected = updateToTransactionRejected,
      convertToContractStateEvents = convertToContractStateEvents,
      updateInMemoryState = inMemoryStateUpdatesCaptor,
    )
    test(inMemoryStateUpdater, inMemoryStateUpdates)
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
    contractMetadata = Map.empty,
  )
  private val metadataChangedUpdate = offset(2L) -> Update.ConfigurationChanged(
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
    contractMetadata = Map.empty,
  )
  private val update4 = offset(4L) -> Update.CommandRejected(
    recordTime = Time.Timestamp.assertFromLong(1337L),
    completionInfo = CompletionInfo(
      actAs = List.empty,
      applicationId = Ref.ApplicationId.assertFromString("some-app-id"),
      commandId = Ref.CommandId.assertFromString("cmdId"),
      optDeduplicationPeriod = None,
      submissionId = None,
      statistics = None,
    ),
    reasonTemplate = FinalReason(new Status()),
  )

  private val txLogUpdate1 = TransactionLogUpdate.TransactionAccepted(
    transactionId = "tx1",
    commandId = "",
    workflowId = "",
    effectiveAt = Timestamp.Epoch,
    offset = offset(1L),
    events = Vector(null),
    completionDetails = None,
  )

  private val txLogUpdate3 = TransactionLogUpdate.TransactionAccepted(
    transactionId = "tx3",
    commandId = "",
    workflowId = "",
    effectiveAt = Timestamp.Epoch,
    offset = offset(3L),
    events = Vector(null),
    completionDetails = None,
  )

  private val txRejected = TransactionLogUpdate.TransactionRejected(
    offset = offset(4L),
    completionDetails = CompletionDetails(
      completionStreamResponse = new CompletionStreamResponse(),
      submitters = Set.empty,
    ),
  )

  private val contractStateEvents1 = Vector(
    ContractStateEvent.Archived(
      contractId = ContractId.assertFromString("00" + "00" * 32 + "c0"),
      globalKey = None,
      stakeholders = Set.empty,
      eventOffset = Offset.beforeBegin,
      eventSequentialId = 0L,
    )
  )

  private def offset(idx: Long): Offset = {
    val base = BigInt(1) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }
}
