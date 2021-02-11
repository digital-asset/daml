// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.Instant

import akka.stream.scaladsl.{Flow, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.WorkflowId
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.v1.Update.{
  PublicPackageUploadRejected,
  TransactionAccepted,
}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf.data.{Bytes, ImmArray, Time}
import com.daml.lf.transaction.{BlindingInfo, NodeId, TransactionVersion, VersionedTransaction}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.{crypto, transaction}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.indexer.ExecuteUpdate.ExecuteUpdateFlow
import com.daml.platform.indexer.OffsetUpdate.PreparedTransactionInsert
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.events.TransactionsWriter
import com.daml.platform.store.dao.{LedgerDao, PersistenceResponse}
import com.daml.platform.store.entries.PackageLedgerEntry
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.OneInstancePerTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

final class ExecuteUpdateSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with OneInstancePerTest
    with TestResourceContext
    with AkkaBeforeAndAfterAll {
  private val loggingContext = LoggingContext.ForTesting

  private val noOpUpdateFlow = Flow[OffsetUpdate].map(_ => ())

  private val mockedPreparedInsert = mock[TransactionsWriter.PreparedInsert]
  private val offset = Offset(Bytes.assertFromString("01"))
  private val txId = TransactionId.fromInt(1)
  private val txMock = transaction.CommittedTransaction(
    VersionedTransaction[NodeId, ContractId](TransactionVersion.VDev, Map.empty, ImmArray.empty)
  )
  private val someMetrics = new Metrics(new MetricRegistry)
  private val someParticipantId = ParticipantId.assertFromString("some-participant")
  private val prepareUpdateParallelism = 2
  private val ledgerEffectiveTime = Instant.EPOCH

  private val packageUploadRejectionReason = "some rejection reason"
  private val submissionId = SubmissionId.assertFromString("s1")
  private val packageUploadRejectedEntry = PackageLedgerEntry.PackageUploadRejected(
    submissionId,
    ledgerEffectiveTime,
    packageUploadRejectionReason,
  )

  private val txAccepted = transactionAccepted(
    submitterInfo = None,
    workflowId = None,
    transactionId = txId,
    ledgerEffectiveTime = Instant.EPOCH,
    transaction = txMock,
    divulgedContracts = List.empty,
    blindingInfo = None,
  )

  private val currentOffset = CurrentOffset(offset = offset)
  private val transactionAcceptedOffsetPair = OffsetUpdate(currentOffset, txAccepted)
  private val packageUploadRejected = PublicPackageUploadRejected(
    submissionId = submissionId,
    recordTime = Time.Timestamp(ledgerEffectiveTime.toEpochMilli),
    rejectionReason = packageUploadRejectionReason,
  )
  private val metadataUpdateOffsetPair = OffsetUpdate(currentOffset, packageUploadRejected)

  private val ledgerDaoMock = {
    val dao = mock[LedgerDao]

    when(
      dao.prepareTransactionInsert(
        submitterInfo = None,
        workflowId = None,
        transactionId = txId,
        ledgerEffectiveTime = ledgerEffectiveTime,
        offset = offset,
        transaction = txMock,
        divulgedContracts = List.empty[DivulgedContract],
        blindingInfo = None,
      )
    ).thenReturn(mockedPreparedInsert)

    when(dao.storeTransactionEvents(mockedPreparedInsert)(loggingContext))
      .thenReturn(Future.successful(PersistenceResponse.Ok))
    when(
      dao.completeTransaction(
        eqTo(Option.empty[SubmitterInfo]),
        eqTo(txId),
        eqTo(ledgerEffectiveTime),
        eqTo(CurrentOffset(offset)),
      )(any[LoggingContext])
    )
      .thenReturn(Future.successful(PersistenceResponse.Ok))
    when(
      dao.storePackageEntry(
        eqTo(currentOffset),
        eqTo(List.empty),
        eqTo(Some(packageUploadRejectedEntry)),
      )(any[LoggingContext])
    ).thenReturn(Future.successful(PersistenceResponse.Ok))
    when(
      dao.storeTransaction(
        preparedInsert = eqTo(mockedPreparedInsert),
        submitterInfo = eqTo(Option.empty[SubmitterInfo]),
        transactionId = eqTo(txId),
        recordTime = eqTo(ledgerEffectiveTime),
        ledgerEffectiveTime = eqTo(ledgerEffectiveTime),
        offsetStep = eqTo(CurrentOffset(offset)),
        transaction = eqTo(txMock),
        divulged = eqTo(List.empty[DivulgedContract]),
      )(any[LoggingContext])
    ).thenReturn(Future.successful(PersistenceResponse.Ok))
    dao
  }

  private class ExecuteUpdateMock(
      val ledgerDao: LedgerDao,
      val participantId: ParticipantId,
      val metrics: Metrics,
      val loggingContext: LoggingContext,
      val executionContext: ExecutionContext,
      val flow: ExecuteUpdateFlow,
      private[indexer] val updatePreparationParallelism: Int = prepareUpdateParallelism,
  ) extends ExecuteUpdate

  private val executeUpdate = new ExecuteUpdateMock(
    ledgerDaoMock,
    someParticipantId,
    someMetrics,
    loggingContext,
    materializer.executionContext,
    noOpUpdateFlow,
  )

  s"${classOf[ExecuteUpdate].getSimpleName}.owner" when {
    def executeUpdateOwner(dbType: DbType) = ExecuteUpdate.owner(
      dbType,
      ledgerDaoMock,
      someMetrics,
      someParticipantId,
      prepareUpdateParallelism,
      materializer.executionContext,
      loggingContext,
    )

    "called with H2Database type" should {

      s"return a ${classOf[AtomicExecuteUpdate]}" in {
        executeUpdateOwner(DbType.H2Database).use {
          case _: AtomicExecuteUpdate => succeed
          case other => fail(s"Unexpected ${other.getClass.getSimpleName}")
        }
      }
    }

    "called with Postgres type" should {
      s"return a ${classOf[PipelinedExecuteUpdate]}" in {
        executeUpdateOwner(DbType.Postgres).use {
          case _: PipelinedExecuteUpdate => succeed
          case other => fail(s"Unexpected ${other.getClass.getSimpleName}")
        }
      }
    }
  }

  "prepareUpdate" when {
    "receives a TransactionAccepted" should {
      "prepare a transaction insert" in {

        val eventualPreparedUpdate = executeUpdate.prepareUpdate(transactionAcceptedOffsetPair)

        eventualPreparedUpdate.map {
          case OffsetUpdate.PreparedTransactionInsert(offsetStep, update, preparedInsert) =>
            offsetStep shouldBe currentOffset
            update shouldBe txAccepted
            preparedInsert shouldBe mockedPreparedInsert
          case _ => fail(s"Should be a ${classOf[PreparedTransactionInsert].getSimpleName}")
        }
      }
    }

    "receives a MetadataUpdate" should {
      "return a MetadataUpdateStep" in {
        val someMetadataUpdate = mock[Update]
        val offsetStepUpdatePair = OffsetUpdate(currentOffset, someMetadataUpdate)
        executeUpdate
          .prepareUpdate(offsetStepUpdatePair)
          .map(_ shouldBe OffsetUpdate(currentOffset, someMetadataUpdate))
      }
    }
  }

  classOf[PipelinedExecuteUpdate].getSimpleName when {
    "receives multiple updates including a transaction accepted" should {
      "process the pipelined stages in the correct order" in {
        PipelinedExecuteUpdate
          .owner(
            ledgerDaoMock,
            someMetrics,
            someParticipantId,
            prepareUpdateParallelism,
            executionContext,
            loggingContext,
          )
          .use { executeUpdate =>
            Source
              .fromIterator(() => Iterator(transactionAcceptedOffsetPair, metadataUpdateOffsetPair))
              .via(executeUpdate.flow)
              .run()
              .map { _ =>
                val orderedEvents = inOrder(ledgerDaoMock)

                orderedEvents
                  .verify(ledgerDaoMock)
                  .prepareTransactionInsert(
                    submitterInfo = None,
                    workflowId = None,
                    transactionId = txId,
                    ledgerEffectiveTime = ledgerEffectiveTime,
                    offset = offset,
                    transaction = txMock,
                    divulgedContracts = List.empty[DivulgedContract],
                    blindingInfo = None,
                  )
                orderedEvents
                  .verify(ledgerDaoMock)
                  .storeTransactionEvents(eqTo(mockedPreparedInsert))(any[LoggingContext])
                orderedEvents
                  .verify(ledgerDaoMock)
                  .completeTransaction(
                    eqTo(Option.empty[SubmitterInfo]),
                    eqTo(txId),
                    eqTo(ledgerEffectiveTime),
                    eqTo(CurrentOffset(offset)),
                  )(any[LoggingContext])
                orderedEvents
                  .verify(ledgerDaoMock)
                  .storePackageEntry(
                    eqTo(currentOffset),
                    eqTo(List.empty),
                    eqTo(Some(packageUploadRejectedEntry)),
                  )(any[LoggingContext])

                verifyNoMoreInteractions(ledgerDaoMock)

                succeed
              }
          }
      }
    }
  }

  classOf[AtomicExecuteUpdate].getSimpleName when {
    "receives multiple updates including a transaction accepted" should {
      "execute all updates atomically" in {
        AtomicExecuteUpdate
          .owner(
            ledgerDaoMock,
            someMetrics,
            someParticipantId,
            prepareUpdateParallelism,
            executionContext,
            loggingContext,
          )
          .use { executeUpdate =>
            Source
              .fromIterator(() => Iterator(transactionAcceptedOffsetPair, metadataUpdateOffsetPair))
              .via(executeUpdate.flow)
              .run()
              .map { _ =>
                val orderedEvents = inOrder(ledgerDaoMock)

                orderedEvents
                  .verify(ledgerDaoMock)
                  .prepareTransactionInsert(
                    submitterInfo = None,
                    workflowId = None,
                    transactionId = txId,
                    ledgerEffectiveTime = ledgerEffectiveTime,
                    offset = offset,
                    transaction = txMock,
                    divulgedContracts = List.empty[DivulgedContract],
                    blindingInfo = None,
                  )
                orderedEvents
                  .verify(ledgerDaoMock)
                  .storeTransaction(
                    preparedInsert = eqTo(mockedPreparedInsert),
                    submitterInfo = eqTo(Option.empty[SubmitterInfo]),
                    transactionId = eqTo(txId),
                    recordTime = eqTo(ledgerEffectiveTime),
                    ledgerEffectiveTime = eqTo(ledgerEffectiveTime),
                    offsetStep = eqTo(CurrentOffset(offset)),
                    transaction = eqTo(txMock),
                    divulged = eqTo(List.empty[DivulgedContract]),
                  )(any[LoggingContext])
                orderedEvents
                  .verify(ledgerDaoMock)
                  .storePackageEntry(
                    eqTo(currentOffset),
                    eqTo(List.empty),
                    eqTo(Some(packageUploadRejectedEntry)),
                  )(any[LoggingContext])

                verifyNoMoreInteractions(ledgerDaoMock)

                succeed
              }
          }
      }
    }
  }

  private def transactionAccepted(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      transaction: CommittedTransaction,
      divulgedContracts: List[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  ): TransactionAccepted = {
    val ledgerTimestamp = Time.Timestamp(ledgerEffectiveTime.toEpochMilli)
    TransactionAccepted(
      optSubmitterInfo = submitterInfo,
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = ledgerTimestamp,
        workflowId = workflowId,
        submissionTime = ledgerTimestamp,
        submissionSeed = crypto.Hash.hashPrivateKey("dummy"),
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      transaction = transaction,
      transactionId = transactionId,
      recordTime = ledgerTimestamp,
      divulgedContracts = divulgedContracts,
      blindingInfo = blindingInfo,
    )
  }
}
