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
import com.daml.platform.indexer.OffsetUpdate.{BatchedTransactions, OffsetUpdateImpl}
import com.daml.platform.indexer.PipelinedExecuteUpdate.`enriched flow`
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

  private[indexer] def updateMetadata(
      metadataUpdateStep: OffsetUpdate
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
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
          ledgerDao.prepareTransactionInsert(offsetStep, Seq(TransactionEntry(offsetStep, tx)))
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

  // TODO Tudor - figure out how to bring logging on par with non-batched insert
//  private def loggingContextForTransactionBatch(batch: Seq[TransactionEntry]): Map[String, String] = {
//      val lastTransactionInBatch = batch.last
//      Map(
//        "updateTransactionIds" -> batch.map(_.transactionId).mkString(","),
//        "updateLedgerTime" -> lastTransactionInBatch.ledgerEffectiveTime.toString,
//        "updateWorkflowIds" -> batch.map(_.workflowId.getOrElse("")).mkString(","),
//      )
//  }

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
    private[indexer] val maximumBatchWindowDuration: FiniteDuration = 100.millis,
    private[indexer] val maximumBatchSize: Int = 100,
)(implicit val executionContext: ExecutionContext, val loggingContext: LoggingContext)
    extends ExecuteUpdate {

  private[indexer] val flow: ExecuteUpdateFlow =
    Flow[OffsetUpdate]
      .groupedWithin(maximumBatchSize, maximumBatchWindowDuration)
      .map(delimitBatches)
      .flatMapConcat(batches => Source.fromIterator(() => batches.iterator))
      .mapAsync(updatePreparationParallelism)(prepareInsertion)
      .mapAsync(1)(insertTransactionState)
      .backPressuredInstrumentedBuffer(
        updatePreparationParallelism,
        metrics.daml.index.db.storeTransactionDbMetrics.transactionEventsBuffer,
      )
      .mapAsync(1)(insertTransactionEvents)
      .mapAsync(1)(completeInsertion)
      .map(_ => ())

  private def delimitBatches(seq: Seq[OffsetUpdate]): Seq[OffsetUpdate] =
    seq
      .foldLeft(List.empty[OffsetUpdate]) {
        case (Nil, OffsetUpdate(offsetStep, tx: TransactionAccepted)) =>
          BatchedTransactions(Vector((offsetStep, tx))) :: Nil
        case (Nil, offsetUpdate) => offsetUpdate :: Nil
        case (
              BatchedTransactions(batch) :: tl,
              OffsetUpdate(offsetStep, tx: TransactionAccepted),
            ) =>
          BatchedTransactions(batch :+ (offsetStep -> tx)) :: tl
        case (batches, OffsetUpdate(offsetStep, tx: TransactionAccepted)) =>
          BatchedTransactions(Vector((offsetStep, tx))) :: batches
        case (batches, offsetUpdate) => offsetUpdate :: batches
      }
      .reverse
      .toVector

  private def prepareInsertion(offsetUpdate: OffsetUpdate): Future[InsertionStageUpdate] =
    offsetUpdate match {
      case BatchedTransactions(batch) =>
        Timed
          .future(
            metrics.daml.index.db.storeTransactionDbMetrics.prepareBatches,
            Future {
              metrics.daml.index.db.storeTransactionBatchSize.mark(batch.size.toLong)
              ledgerDao.prepareTransactionInsert(
                batchOffsetStep(batch),
                batch.map { case (offsetStep, tx) =>
                  TransactionEntry(offsetStep, tx)
                },
              )
            },
          )
          .map(PreparedTransactionInsert(_, batch.size))
      case offsetUpdate: OffsetUpdateImpl => Future(offsetUpdate)
    }

  private def batchOffsetStep[T](updates: Seq[(OffsetStep, T)]): OffsetStep =
    updates match {
      case Seq() => throw new RuntimeException("TransactionAccepted batch cannot be empty")
      case Seq((offsetStep, _)) => offsetStep
      case multipleUpdates =>
        (multipleUpdates.head._1, multipleUpdates.last._1) match {
          case (_: CurrentOffset, IncrementalOffsetStep(_, lastOffset)) => CurrentOffset(lastOffset)
          case (
                IncrementalOffsetStep(lastUpdatedOffset, _),
                IncrementalOffsetStep(_, lastOffset),
              ) =>
            IncrementalOffsetStep(lastUpdatedOffset, lastOffset)
          case invalidOffsetCombination =>
            throw new RuntimeException(
              s"Invalid batch offset combination encountered when constructing batch offset step: $invalidOffsetCombination"
            )
        }
    }

  private def insertTransactionState(
      insertionStageUpdate: InsertionStageUpdate
  ): Future[InsertionStageUpdate] =
    insertionStageUpdate match {
      case PreparedTransactionInsert(preparedInsert, batchSize) =>
        normalizedTimed(
          metrics.daml.index.db.storeTransactionState,
          ledgerDao.storeTransactionState(preparedInsert).map(_ => insertionStageUpdate),
          batchSize,
        )
      case _ => Future.successful(insertionStageUpdate)
    }

  private def insertTransactionEvents(
      insertionStageUpdate: InsertionStageUpdate
  ): Future[InsertionStageUpdate] =
    insertionStageUpdate match {
      case PreparedTransactionInsert(preparedInsert, batchSize) =>
        normalizedTimed(
          metrics.daml.index.db.storeTransactionEvents,
          ledgerDao
            .storeTransactionEvents(preparedInsert)
            .map(_ => insertionStageUpdate),
          batchSize = batchSize,
        )
      case _ => Future.successful(insertionStageUpdate)
    }

  private def completeInsertion(
      insertionStageUpdate: InsertionStageUpdate
  ): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.indexer.stateUpdateProcessing, {
        insertionStageUpdate match {
          case PreparedTransactionInsert(preparedInsert, batchSize) =>
            // TODO Tudor fix logging context
            withEnrichedLoggingContext(Map.empty[String, String]) { implicit loggingContext =>
              completeTransactionInsertion(preparedInsert, batchSize)
            }
          case offsetUpdate @ OffsetUpdate(offsetStep, update) =>
            withEnrichedLoggingContext(
              loggingContextFor(offsetStep.offset, update)
            ) { implicit loggingContext =>
              updateMetadata(offsetUpdate)
            }
        }
      },
    )

  private def completeTransactionInsertion(
      preparedInsert: PreparedInsert,
      batchSize: Int,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    normalizedTimed(
      metrics.daml.index.db.storeTransactionCompletion,
      ledgerDao.completeTransaction(preparedInsert),
      batchSize,
    )

  def normalizedTimed[T](timer: Timer, future: => Future[T], batchSize: Int): Future[T] = {
    val start = System.nanoTime()
    val result = future
    result.onComplete(_ =>
      timer.update((System.nanoTime() - start) / batchSize, TimeUnit.NANOSECONDS)
    )(com.daml.dec.DirectExecutionContext)
    result
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
      offsetUpdate: OffsetUpdate
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    offsetUpdate match {
      case OffsetUpdateImpl(offsetStep, transactionAccepted: TransactionAccepted) =>
        val preparedInsert =
          Timed.value(
            metrics.daml.index.db.storeTransactionDbMetrics.prepareBatches,
            ledgerDao.prepareTransactionInsert(
              offsetStep,
              Seq(TransactionEntry(offsetStep, transactionAccepted)),
            ),
          )
        Timed.future(
          metrics.daml.index.db.storeTransaction,
          ledgerDao.storeTransaction(
            preparedInsert,
            submitterInfo = transactionAccepted.optSubmitterInfo,
            transactionId = transactionAccepted.transactionId,
            recordTime = transactionAccepted.recordTime.toInstant,
            ledgerEffectiveTime = transactionAccepted.transactionMeta.ledgerEffectiveTime.toInstant,
            offsetStep = offsetStep,
            transaction = transactionAccepted.transaction,
            divulged = transactionAccepted.divulgedContracts,
          ),
        )
      case metadataUpdate: OffsetUpdateImpl => updateMetadata(metadataUpdate)
      case _: BatchedTransactions =>
        Future.failed[PersistenceResponse](
          new Exception(
            "This is likely an error. Batch transactions are not supported for atomic updates."
          )
        )
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
