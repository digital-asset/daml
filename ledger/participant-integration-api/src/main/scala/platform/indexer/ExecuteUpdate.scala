// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import com.codahale.metrics.Timer
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1.{Offset, Party, Update}
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.indexer.ExecuteUpdate.ExecuteUpdateFlow
import com.daml.platform.indexer.OffsetUpdate.PreparedTransactionInsert
import com.daml.platform.indexer.PipelinedExecuteUpdate.PipelinedUpdateWithTimer
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.{LedgerDao, PersistenceResponse}
import com.daml.platform.store.entries.{PackageLedgerEntry, PartyLedgerEntry}

import scala.concurrent.{ExecutionContext, Future}

object ExecuteUpdate {
  type ExecuteUpdateFlow = Flow[OffsetUpdate, Unit, NotUsed]
  type FlowOwnerBuilder =
    (
        DbType,
        LedgerDao,
        Metrics,
        v1.ParticipantId,
        ExecutionContext,
        LoggingContext,
    ) => ResourceOwner[ExecuteUpdate]

  def owner(
      dbType: DbType,
      ledgerDao: LedgerDao,
      metrics: Metrics,
      participantId: v1.ParticipantId,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[ExecuteUpdate] =
    dbType match {
      case DbType.Postgres =>
        PipelinedExecuteUpdate.owner(
          ledgerDao,
          metrics,
          participantId,
          executionContext,
          loggingContext,
        )
      case DbType.H2Database =>
        AtomicExecuteUpdate.owner(
          ledgerDao,
          metrics,
          participantId,
          executionContext,
          loggingContext,
        )
    }
}

trait ExecuteUpdate {
  private val logger = ContextualizedLogger.get(this.getClass)

  private[indexer] implicit val loggingContext: LoggingContext
  private[indexer] implicit val executionContext: ExecutionContext

  private[indexer] def participantId: v1.ParticipantId

  private[indexer] def ledgerDao: LedgerDao

  private[indexer] def metrics: Metrics

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
              submitterInfo = tx.optSubmitterInfo,
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
        val packages: List[(DamlLf.Archive, v2.PackageDetails)] = archives.map(archive =>
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

      case CommandRejected(recordTime, submitterInfo, reason) =>
        ledgerDao.storeRejection(Some(submitterInfo), recordTime.toInstant, offsetStep, reason)
      case update: TransactionAccepted =>
        import update._
        logger.warn(
          """For performance considerations, TransactionAccepted should be handled in the prepare insert stage.
            |Recomputing PreparedInsert..""".stripMargin
        )
        ledgerDao.storeTransaction(
          preparedInsert = ledgerDao.prepareTransactionInsert(
            submitterInfo = optSubmitterInfo,
            workflowId = transactionMeta.workflowId,
            transactionId = transactionId,
            ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
            offset = offsetStep.offset,
            transaction = transaction,
            divulgedContracts = divulgedContracts,
            blindingInfo = blindingInfo,
          ),
          submitterInfo = optSubmitterInfo,
          transactionId = transactionId,
          recordTime = recordTime.toInstant,
          ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
          offsetStep = offsetStep,
          transaction = transaction,
          divulged = divulgedContracts,
        )
    }
  }

  private[indexer] def loggingContextFor(
      offset: Offset,
      update: Update,
  ): Map[String, String] =
    loggingContextFor(update)
      .updated("updateRecordTime", update.recordTime.toInstant.toString)
      .updated("updateOffset", offset.toHexString)

  private def loggingContextFor(update: Update): Map[String, String] =
    update match {
      case ConfigurationChanged(_, submissionId, participantId, newConfiguration) =>
        Map(
          "updateSubmissionId" -> submissionId,
          "updateParticipantId" -> participantId,
          "updateConfigGeneration" -> newConfiguration.generation.toString,
          "updatedMaxDeduplicationTime" -> newConfiguration.maxDeduplicationTime.toString,
        )
      case ConfigurationChangeRejected(
            _,
            submissionId,
            participantId,
            proposedConfiguration,
            rejectionReason,
          ) =>
        Map(
          "updateSubmissionId" -> submissionId,
          "updateParticipantId" -> participantId,
          "updateConfigGeneration" -> proposedConfiguration.generation.toString,
          "updatedMaxDeduplicationTime" -> proposedConfiguration.maxDeduplicationTime.toString,
          "updateRejectionReason" -> rejectionReason,
        )
      case PartyAddedToParticipant(party, displayName, participantId, _, submissionId) =>
        Map(
          "updateSubmissionId" -> submissionId.getOrElse(""),
          "updateParticipantId" -> participantId,
          "updateParty" -> party,
          "updateDisplayName" -> displayName,
        )
      case PartyAllocationRejected(submissionId, participantId, _, rejectionReason) =>
        Map(
          "updateSubmissionId" -> submissionId,
          "updateParticipantId" -> participantId,
          "updateRejectionReason" -> rejectionReason,
        )
      case PublicPackageUpload(_, sourceDescription, _, submissionId) =>
        Map(
          "updateSubmissionId" -> submissionId.getOrElse(""),
          "updateSourceDescription" -> sourceDescription.getOrElse(""),
        )
      case PublicPackageUploadRejected(submissionId, _, rejectionReason) =>
        Map(
          "updateSubmissionId" -> submissionId,
          "updateRejectionReason" -> rejectionReason,
        )
      case TransactionAccepted(optSubmitterInfo, transactionMeta, _, transactionId, _, _, _) =>
        Map(
          "updateTransactionId" -> transactionId,
          "updateLedgerTime" -> transactionMeta.ledgerEffectiveTime.toInstant.toString,
          "updateWorkflowId" -> transactionMeta.workflowId.getOrElse(""),
          "updateSubmissionTime" -> transactionMeta.submissionTime.toInstant.toString,
        ) ++ optSubmitterInfo
          .map(info =>
            Map(
              "updateSubmitter" -> loggingContextPartiesValue(info.actAs),
              "updateApplicationId" -> info.applicationId,
              "updateCommandId" -> info.commandId,
              "updateDeduplicateUntil" -> info.deduplicateUntil.toString,
            )
          )
          .getOrElse(Map.empty)
      case CommandRejected(_, submitterInfo, reason) =>
        Map(
          "updateSubmitter" -> loggingContextPartiesValue(submitterInfo.actAs),
          "updateApplicationId" -> submitterInfo.applicationId,
          "updateCommandId" -> submitterInfo.commandId,
          "updateDeduplicateUntil" -> submitterInfo.deduplicateUntil.toString,
          "updateRejectionReason" -> reason.description,
        )
    }

  private def loggingContextPartiesValue(parties: List[Party]): String =
    parties.mkString("[", ", ", "]")
}

class PipelinedExecuteUpdate(
    private[indexer] val ledgerDao: LedgerDao,
    private[indexer] val metrics: Metrics,
    private[indexer] val participantId: v1.ParticipantId,
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
      loggingContextFor(pipelinedUpdate.offsetStep.offset, pipelinedUpdate.update)
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
      tx: TransactionAccepted,
      pipelinedInsertTimer: Timer.Context,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed
      .future(
        metrics.daml.index.db.storeTransactionCompletion,
        ledgerDao.completeTransaction(
          submitterInfo = tx.optSubmitterInfo,
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
      .mapAsync(2)(prepareUpdate)
      .buffer(16, OverflowStrategy.backpressure)
      .map(PipelinedUpdateWithTimer(_, metrics.daml.index.db.storeTransaction.time()))
      .mapAsync(1)(insertTransactionState)
      .mapAsync(1)(insertTransactionEvents)
      .mapAsync(1)(completeInsertion)
      .map(_ => ())
}

object PipelinedExecuteUpdate {
  def owner(
      ledgerDao: LedgerDao,
      metrics: Metrics,
      participantId: v1.ParticipantId,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[PipelinedExecuteUpdate] =
    ResourceOwner.forValue(() =>
      new PipelinedExecuteUpdate(ledgerDao, metrics, participantId)(
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
    private[indexer] val ledgerDao: LedgerDao,
    private[indexer] val metrics: Metrics,
    private[indexer] val participantId: v1.ParticipantId,
)(
    private[indexer] implicit val loggingContext: LoggingContext,
    private[indexer] val executionContext: ExecutionContext,
) extends ExecuteUpdate {

  private[indexer] val flow: ExecuteUpdateFlow =
    Flow[OffsetUpdate]
      .mapAsync(1)(prepareUpdate)
      .mapAsync(1) { case offsetUpdate @ OffsetUpdate(offsetStep, update) =>
        withEnrichedLoggingContext(loggingContextFor(offsetStep.offset, update)) {
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
            TransactionAccepted(
              optSubmitterInfo,
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
            submitterInfo = optSubmitterInfo,
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
      ledgerDao: LedgerDao,
      metrics: Metrics,
      participantId: v1.ParticipantId,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[AtomicExecuteUpdate] =
    ResourceOwner.forValue(() =>
      new AtomicExecuteUpdate(ledgerDao, metrics, participantId)(loggingContext, executionContext)
    )
}
