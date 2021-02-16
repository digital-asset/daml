// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Source}
import com.codahale.metrics.{Counter, Timer}
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1.{Offset, Party, Update}
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.indexer.ExecuteUpdate.ExecuteUpdateFlow
import com.daml.platform.indexer.OffsetUpdate.{BatchedTransactions, PreparedTransactionInsert}
import com.daml.platform.indexer.PipelinedExecuteUpdate.{PipelinedUpdateWithTimer, `enriched flow`}
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.events.TransactionEntry
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert
import com.daml.platform.store.dao.{LedgerDao, PersistenceResponse}
import com.daml.platform.store.entries.{PackageLedgerEntry, PartyLedgerEntry}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object ExecuteUpdate {
  type ExecuteUpdateFlow = Flow[OffsetUpdate, Unit, NotUsed]
  type FlowOwnerBuilder =
    (
        DbType,
        LedgerDao,
        Metrics,
        v1.ParticipantId,
        Int,
        ExecutionContext,
        LoggingContext,
    ) => ResourceOwner[ExecuteUpdate]

  def owner(
      dbType: DbType,
      ledgerDao: LedgerDao,
      metrics: Metrics,
      participantId: v1.ParticipantId,
      updatePreparationParallelism: Int,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[ExecuteUpdate] =
    dbType match {
      case DbType.Postgres =>
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
            val preparedInsert = ledgerDao
              .prepareTransactionInsert(
                Seq(
                  TransactionEntry(
                    offset = offsetStepPair.offsetStep.offset,
                    transactionAccepted = tx,
                  )
                )
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
      case tx: TransactionAccepted =>
        import tx._
        logger.warn(
          """For performance considerations, TransactionAccepted should be handled in the prepare insert stage.
            |Recomputing PreparedInsert..""".stripMargin
        )
        val preparedInsert =
          ledgerDao.prepareTransactionInsert(Seq(TransactionEntry(offsetStep.offset, tx)))
        ledgerDao.storeTransaction(
          preparedInsert = preparedInsert,
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
    private[indexer] val updatePreparationParallelism: Int,
)(implicit val executionContext: ExecutionContext, val loggingContext: LoggingContext)
    extends ExecuteUpdate {

  private def delimitBatches(seq: Seq[OffsetUpdate]): Seq[OffsetUpdate] =
    seq
      .foldLeft(List.empty[OffsetUpdate]) {
        case (Nil, OffsetUpdate(offset, tx: TransactionAccepted)) =>
          BatchedTransactions(offset, tx, Vector((offset.offset, tx))) :: Nil
        case (Nil, offsetUpdate) => offsetUpdate :: Nil
        case (
              BatchedTransactions(_, _, batch) :: tl,
              OffsetUpdate(offset, tx: TransactionAccepted),
            ) =>
          BatchedTransactions(offset, tx, batch :+ (offset.offset -> tx)) :: tl
        case (batches, OffsetUpdate(offset, tx: TransactionAccepted)) =>
          BatchedTransactions(offset, tx, Vector((offset.offset, tx))) :: batches
        case (batches, offsetUpdate) => offsetUpdate :: batches
      }
      .reverse
      .toVector

  private def insertTransactionState(
      timedPipelinedUpdate: PipelinedUpdateWithTimer
  ): Future[PipelinedUpdateWithTimer] = timedPipelinedUpdate.preparedUpdate match {
    case PreparedTransactionInsert(_, _, preparedInsert) =>
      futureBatch(
        metrics.daml.index.db.storeTransactionState,
        ledgerDao.storeTransactionState(preparedInsert).map(_ => timedPipelinedUpdate),
        timedPipelinedUpdate.batchSize,
      )
    case _ => Future.successful(timedPipelinedUpdate)
  }

  private def insertTransactionEvents(
      timedPipelinedUpdate: PipelinedUpdateWithTimer
  ): Future[PipelinedUpdateWithTimer] = timedPipelinedUpdate.preparedUpdate match {
    case PreparedTransactionInsert(_, _, preparedInsert) =>
      futureBatch(
        metrics.daml.index.db.storeTransactionEvents,
        ledgerDao
          .storeTransactionEvents(preparedInsert)
          .map(_ => timedPipelinedUpdate),
        batchSize = timedPipelinedUpdate.batchSize,
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
            case OffsetUpdate.PreparedTransactionInsert(_, _, preparedInsert) =>
              completeTransactionInsertion(preparedInsert, timedPipelinedUpdate.batchSize)
            case metadataUpdate => updateMetadata(metadataUpdate)
          }
        },
      )
    }
  }

  private def completeTransactionInsertion(
      preparedInsert: PreparedInsert,
      batchSize: Int,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    futureBatch(
      metrics.daml.index.db.storeTransactionCompletion,
      ledgerDao.completeTransaction(preparedInsert),
      batchSize,
    )

  def futureBatch[T](timer: Timer, future: => Future[T], batchSize: Int): Future[T] = {
    val start = System.nanoTime()
    val result = future
    result.onComplete(_ =>
      timer.update((System.nanoTime() - start) / batchSize, TimeUnit.NANOSECONDS)
    )(com.daml.dec.DirectExecutionContext)
    result
  }

  private[indexer] val flow: ExecuteUpdateFlow = {
    import metrics.daml.index.db.storeTransactionDbMetrics
    Flow[OffsetUpdate]
      .groupedWithin(25, 25.millis)
      .map(delimitBatches)
      .flatMapConcat(batches => Source.fromIterator(() => batches.iterator))
      .mapAsync(4) {
        case pb @ BatchedTransactions(offsetStep, update, _) =>
          Future {
            metrics.daml.index.db.storeTransactionBatchSize.mark(pb.batch.size.toLong)
            ledgerDao.prepareTransactionInsert(pb.batch.map { case (offset, tx) =>
              TransactionEntry(offset, tx)
            })
          }
            .map(preparedInsert =>
              PipelinedUpdateWithTimer(
                PreparedTransactionInsert(offsetStep, update, preparedInsert),
                pb.batch.size,
              )
            )
        case offsetUpdate =>
          Future.successful(PipelinedUpdateWithTimer(offsetUpdate, 0))
      }
      .mapAsync(1)(insertTransactionState)
      .backPressuredInstrumentedBuffer(4, storeTransactionDbMetrics.transactionEventsBuffer)
      .mapAsync(1)(insertTransactionEvents)
      .mapAsync(1)(completeInsertion)
      .map(_ => ())
  }
}

object PipelinedExecuteUpdate {
  implicit class `enriched flow`[I, O, M](flow: Flow[I, O, M]) {
    def backPressuredInstrumentedBuffer(size: Int, counter: Counter): Flow[I, O, M] =
      flow
        .map { el =>
          counter.inc()
          el
        }
        .buffer(size, OverflowStrategy.backpressure)
        .map { el =>
          counter.dec()
          el
        }
  }

  def owner(
      ledgerDao: LedgerDao,
      metrics: Metrics,
      participantId: v1.ParticipantId,
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
      batchSize: Int,
  )

}

class AtomicExecuteUpdate(
    private[indexer] val ledgerDao: LedgerDao,
    private[indexer] val metrics: Metrics,
    private[indexer] val participantId: v1.ParticipantId,
    private[indexer] val updatePreparationParallelism: Int,
)(
    private[indexer] implicit val loggingContext: LoggingContext,
    private[indexer] val executionContext: ExecutionContext,
) extends ExecuteUpdate {

  private[indexer] val flow: ExecuteUpdateFlow =
    Flow[OffsetUpdate]
      .mapAsync(updatePreparationParallelism)(prepareUpdate)
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
