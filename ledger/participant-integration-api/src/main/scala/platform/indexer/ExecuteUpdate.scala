// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.codahale.metrics.Timer
import com.daml.daml_lf.ArchiveOuterClass.Archive
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.indexer.ExecuteUpdate.ExecuteUpdateFlow
import com.daml.platform.indexer.OffsetUpdate.PreparedTransactionInsert
import com.daml.platform.indexer.PipelinedExecuteUpdate.PipelinedUpdateWithTimer
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.{LedgerWriteDao, PersistenceResponse}
import com.daml.platform.store.entries.{PackageLedgerEntry, PartyLedgerEntry}

import scala.concurrent.{ExecutionContext, Future}

object ExecuteUpdate {
  type ExecuteUpdateFlow = Flow[OffsetUpdate, Unit, NotUsed]
  type FlowOwnerBuilder =
    (
        DbType,
        LedgerWriteDao,
        Metrics,
        Ref.ParticipantId,
        Int,
        ExecutionContext,
        LoggingContext,
    ) => ResourceOwner[ExecuteUpdate]

  def owner(
      dbType: DbType,
      ledgerDao: LedgerWriteDao,
      metrics: Metrics,
      participantId: Ref.ParticipantId,
      updatePreparationParallelism: Int,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[ExecuteUpdate] =
    dbType match {
      case DbType.Postgres | DbType.Oracle =>
        PipelinedExecuteUpdate.owner(
          ledgerDao,
          metrics,
          participantId,
          updatePreparationParallelism,
          executionContext,
          loggingContext,
        )
      case DbType.H2Database =>
        AtomicExecuteUpdate.owner(
          ledgerDao,
          metrics,
          participantId,
          updatePreparationParallelism,
          executionContext,
          loggingContext,
        )
    }
}

trait ExecuteUpdate {
  import state.Update._

  private val logger = ContextualizedLogger.get(this.getClass)

  private[indexer] implicit val loggingContext: LoggingContext
  private[indexer] implicit val executionContext: ExecutionContext

  private[indexer] def participantId: Ref.ParticipantId

  private[indexer] def ledgerDao: LedgerWriteDao

  private[indexer] def metrics: Metrics

  private[indexer] def updatePreparationParallelism: Int

  private[indexer] def flow: ExecuteUpdateFlow

  private[indexer] def prepareUpdate(
      offsetStepPair: OffsetUpdate
  ): Future[OffsetUpdate] =
    offsetStepPair.update match {
      case tx: TransactionAccepted =>
        Timed.future(
          metrics.daml.index.db.storeTransactionDbMetrics.prepareBatches,
          Future {
            val preparedInsert = ledgerDao.prepareTransactionInsert(
              completionInfo = tx.optCompletionInfo,
              workflowId = tx.transactionMeta.workflowId,
              transactionId = tx.transactionId,
              ledgerEffectiveTime = tx.transactionMeta.ledgerEffectiveTime.toInstant,
              offset = offsetStepPair.offsetStep.offset,
              transaction = tx.transaction,
              divulgedContracts = tx.divulgedContracts,
              blindingInfo = tx.blindingInfo,
            )
            OffsetUpdate.PreparedTransactionInsert(
              offsetStep = offsetStepPair.offsetStep,
              update = tx,
              preparedInsert = preparedInsert,
            )
          },
        )
      case metadataUpdate =>
        Future.successful(OffsetUpdate(offsetStepPair.offsetStep, metadataUpdate))
    }

  private[indexer] def updateMetadata(
      metadataUpdateStep: OffsetUpdate
  ): Future[PersistenceResponse] = {
    val OffsetUpdate(offsetStep, update) = metadataUpdateStep
    update match {
      case PartyAddedToParticipant(
            party,
            displayName,
            hostingParticipantId,
            recordTime,
            submissionId,
          ) =>
        val entry = PartyLedgerEntry.AllocationAccepted(
          submissionId,
          recordTime.toInstant,
          domain.PartyDetails(party, Some(displayName), participantId == hostingParticipantId),
        )
        ledgerDao.storePartyEntry(offsetStep, entry)

      case PartyAllocationRejected(
            submissionId,
            _,
            recordTime,
            rejectionReason,
          ) =>
        val entry = PartyLedgerEntry.AllocationRejected(
          submissionId,
          recordTime.toInstant,
          rejectionReason,
        )
        ledgerDao.storePartyEntry(offsetStep, entry)

      case PublicPackageUpload(archives, optSourceDescription, recordTime, optSubmissionId) =>
        val recordTimeInstant = recordTime.toInstant
        val packages: List[(Archive, v2.PackageDetails)] = archives.map(archive =>
          archive -> v2.PackageDetails(
            size = archive.getPayload.size.toLong,
            knownSince = recordTimeInstant,
            sourceDescription = optSourceDescription,
          )
        )
        val optEntry: Option[PackageLedgerEntry] =
          optSubmissionId.map(submissionId =>
            PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTimeInstant)
          )
        ledgerDao.storePackageEntry(offsetStep, packages, optEntry)

      case PublicPackageUploadRejected(submissionId, recordTime, rejectionReason) =>
        val entry = PackageLedgerEntry.PackageUploadRejected(
          submissionId,
          recordTime.toInstant,
          rejectionReason,
        )
        ledgerDao.storePackageEntry(offsetStep, List.empty, Some(entry))

      case config: ConfigurationChanged =>
        ledgerDao.storeConfigurationEntry(
          offsetStep,
          config.recordTime.toInstant,
          config.submissionId,
          config.newConfiguration,
          None,
        )

      case configRejection: ConfigurationChangeRejected =>
        ledgerDao.storeConfigurationEntry(
          offsetStep,
          configRejection.recordTime.toInstant,
          configRejection.submissionId,
          configRejection.proposedConfiguration,
          Some(configRejection.rejectionReason),
        )

      case CommandRejected(recordTime, completionInfo, reason) =>
        ledgerDao.storeRejection(
          Some(completionInfo),
          recordTime.toInstant,
          offsetStep,
          reason,
        )
      case update: TransactionAccepted =>
        import update._
        logger.warn(
          """For performance considerations, TransactionAccepted should be handled in the prepare insert stage.
            |Recomputing PreparedInsert..""".stripMargin
        )
        ledgerDao.storeTransaction(
          preparedInsert = ledgerDao.prepareTransactionInsert(
            completionInfo = optCompletionInfo,
            workflowId = transactionMeta.workflowId,
            transactionId = transactionId,
            ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
            offset = offsetStep.offset,
            transaction = transaction,
            divulgedContracts = divulgedContracts,
            blindingInfo = blindingInfo,
          ),
          completionInfo = optCompletionInfo,
          transactionId = transactionId,
          recordTime = recordTime.toInstant,
          ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
          offsetStep = offsetStep,
          transaction = transaction,
          divulged = divulgedContracts,
        )
    }
  }
}

class PipelinedExecuteUpdate(
    private[indexer] val ledgerDao: LedgerWriteDao,
    private[indexer] val metrics: Metrics,
    private[indexer] val participantId: Ref.ParticipantId,
    private[indexer] val updatePreparationParallelism: Int,
)(implicit val executionContext: ExecutionContext, val loggingContext: LoggingContext)
    extends ExecuteUpdate {
  private def insertTransactionState(
      timedPipelinedUpdate: PipelinedUpdateWithTimer
  ): Future[PipelinedUpdateWithTimer] = timedPipelinedUpdate.preparedUpdate match {
    case PreparedTransactionInsert(_, _, preparedInsert) =>
      Timed.future(
        metrics.daml.index.db.storeTransactionState,
        ledgerDao.storeTransactionState(preparedInsert).map(_ => timedPipelinedUpdate),
      )
    case _ => Future.successful(timedPipelinedUpdate)
  }

  private def insertTransactionEvents(
      timedPipelinedUpdate: PipelinedUpdateWithTimer
  ): Future[PipelinedUpdateWithTimer] = timedPipelinedUpdate.preparedUpdate match {
    case PreparedTransactionInsert(_, _, preparedInsert) =>
      Timed.future(
        metrics.daml.index.db.storeTransactionEvents,
        ledgerDao
          .storeTransactionEvents(preparedInsert)
          .map(_ => timedPipelinedUpdate),
      )
    case _ => Future.successful(timedPipelinedUpdate)
  }

  private def completeInsertion(
      timedPipelinedUpdate: PipelinedUpdateWithTimer
  ): Future[PersistenceResponse] = {
    val pipelinedUpdate = timedPipelinedUpdate.preparedUpdate
    withEnrichedLoggingContext(
      "offset" -> pipelinedUpdate.offsetStep.offset,
      "update" -> pipelinedUpdate.update,
    ) { implicit loggingContext =>
      Timed.future(
        metrics.daml.indexer.stateUpdateProcessing, {
          pipelinedUpdate match {
            case OffsetUpdate.PreparedTransactionInsert(offsetStep, tx, _) =>
              completeTransactionInsertion(
                offsetStep,
                tx,
                timedPipelinedUpdate.transactionInsertionTimer,
              )
            case metadataUpdate =>
              updateMetadata(metadataUpdate)
          }
        },
      )
    }
  }

  private def completeTransactionInsertion(
      offsetStep: OffsetStep,
      tx: state.Update.TransactionAccepted,
      pipelinedInsertTimer: Timer.Context,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed
      .future(
        metrics.daml.index.db.storeTransactionCompletion,
        ledgerDao.completeTransaction(
          completionInfo = tx.optCompletionInfo,
          transactionId = tx.transactionId,
          recordTime = tx.recordTime.toInstant,
          offsetStep = offsetStep,
        ),
      )
      .transform { response =>
        pipelinedInsertTimer.stop()
        response
      }

  private[indexer] val flow: ExecuteUpdateFlow =
    Flow[OffsetUpdate]
      .mapAsync(updatePreparationParallelism)(prepareUpdate)
      .async
      .map(PipelinedUpdateWithTimer(_, metrics.daml.index.db.storeTransaction.time()))
      .mapAsync(1)(insertTransactionState)
      .async
      .mapAsync(1)(insertTransactionEvents)
      .async
      .mapAsync(1)(completeInsertion)
      .async
      .map(_ => ())
}

object PipelinedExecuteUpdate {
  def owner(
      ledgerDao: LedgerWriteDao,
      metrics: Metrics,
      participantId: Ref.ParticipantId,
      updatePreparationParallelism: Int,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[PipelinedExecuteUpdate] =
    ResourceOwner.forValue(() =>
      new PipelinedExecuteUpdate(ledgerDao, metrics, participantId, updatePreparationParallelism)(
        executionContext,
        loggingContext,
      )
    )

  private case class PipelinedUpdateWithTimer(
      preparedUpdate: OffsetUpdate,
      transactionInsertionTimer: Timer.Context,
  )

}

class AtomicExecuteUpdate(
    private[indexer] val ledgerDao: LedgerWriteDao,
    private[indexer] val metrics: Metrics,
    private[indexer] val participantId: Ref.ParticipantId,
    private[indexer] val updatePreparationParallelism: Int,
)(
    private[indexer] implicit val loggingContext: LoggingContext,
    private[indexer] val executionContext: ExecutionContext,
) extends ExecuteUpdate {
  private[indexer] val flow: ExecuteUpdateFlow =
    Flow[OffsetUpdate]
      .mapAsync(updatePreparationParallelism)(prepareUpdate)
      .mapAsync(1) { case offsetUpdate @ OffsetUpdate(offsetStep, update) =>
        withEnrichedLoggingContext("offset" -> offsetStep.offset, "update" -> update) {
          implicit loggingContext =>
            Timed.future(
              metrics.daml.indexer.stateUpdateProcessing,
              executeUpdate(offsetUpdate),
            )
        }
      }
      .map(_ => ())

  private def executeUpdate(
      preparedUpdate: OffsetUpdate
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    preparedUpdate match {
      case PreparedTransactionInsert(
            offsetStep,
            state.Update.TransactionAccepted(
              optCompletionInfo,
              transactionMeta,
              transaction,
              transactionId,
              recordTime,
              divulgedContracts,
              _,
            ),
            preparedInsert,
          ) =>
        Timed.future(
          metrics.daml.index.db.storeTransaction,
          ledgerDao.storeTransaction(
            preparedInsert,
            completionInfo = optCompletionInfo,
            transactionId = transactionId,
            recordTime = recordTime.toInstant,
            ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
            offsetStep = offsetStep,
            transaction = transaction,
            divulged = divulgedContracts,
          ),
        )

      case metadataUpdate => updateMetadata(metadataUpdate)
    }
}

object AtomicExecuteUpdate {
  def owner(
      ledgerDao: LedgerWriteDao,
      metrics: Metrics,
      participantId: Ref.ParticipantId,
      updatePreparationParallelism: Int,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[AtomicExecuteUpdate] =
    ResourceOwner.forValue(() =>
      new AtomicExecuteUpdate(ledgerDao, metrics, participantId, updatePreparationParallelism)(
        loggingContext,
        executionContext,
      )
    )
}
