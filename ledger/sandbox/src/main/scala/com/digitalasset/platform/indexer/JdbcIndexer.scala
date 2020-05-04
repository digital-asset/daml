// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.Instant

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink}
import com.daml.daml_lf_dev.DamlLf
import com.daml.dec.{DirectExecutionContext => DEC}
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.common.LedgerIdMismatchException
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerDao}
import com.daml.platform.store.entries.{PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.FlywayMigrations
import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final class JdbcIndexerFactory(
    serverRole: ServerRole,
    config: IndexerConfig,
    readService: ReadService,
    metrics: Metrics,
)(implicit materializer: Materializer, logCtx: LoggingContext) {

  private val logger = ContextualizedLogger.get(this.getClass)

  def validateSchema()(
      implicit executionContext: ExecutionContext
  ): Future[ResourceOwner[JdbcIndexer]] =
    new FlywayMigrations(config.jdbcUrl)
      .validate()
      .map(_ => initialized())

  def migrateSchema(allowExistingSchema: Boolean)(
      implicit executionContext: ExecutionContext
  ): Future[ResourceOwner[JdbcIndexer]] =
    new FlywayMigrations(config.jdbcUrl)
      .migrate(allowExistingSchema)
      .map(_ => initialized())

  def resetSchema()(
      implicit executionContext: ExecutionContext
  ): Future[ResourceOwner[JdbcIndexer]] =
    Future.successful(for {
      ledgerDao <- JdbcLedgerDao.writeOwner(
        serverRole,
        config.jdbcUrl,
        config.eventsPageSize,
        metrics,
      )
      _ <- ResourceOwner.forFuture(() => ledgerDao.reset())
      initialLedgerEnd <- ResourceOwner.forFuture(() => initializeLedger(ledgerDao))
    } yield new JdbcIndexer(initialLedgerEnd, config.participantId, ledgerDao, metrics))

  private def initialized()(
      implicit executionContext: ExecutionContext
  ): ResourceOwner[JdbcIndexer] =
    for {
      ledgerDao <- JdbcLedgerDao.writeOwner(
        serverRole,
        config.jdbcUrl,
        config.eventsPageSize,
        metrics,
      )
      initialLedgerEnd <- ResourceOwner.forFuture(() => initializeLedger(ledgerDao))
    } yield new JdbcIndexer(initialLedgerEnd, config.participantId, ledgerDao, metrics)

  private def initializeLedger(dao: LedgerDao)(
      implicit executionContext: ExecutionContext,
  ): Future[Option[Offset]] =
    for {
      initialConditions <- readService.getLedgerInitialConditions().runWith(Sink.head)
      existingLedgerId <- dao.lookupLedgerId()
      providedLedgerId = domain.LedgerId(initialConditions.ledgerId)
      _ <- existingLedgerId.fold(initializeLedgerData(providedLedgerId, dao))(
        checkLedgerIds(_, providedLedgerId))
      initialLedgerEnd <- dao.lookupInitialLedgerEnd()
    } yield initialLedgerEnd

  private def checkLedgerIds(
      existingLedgerId: domain.LedgerId,
      providedLedgerId: domain.LedgerId,
  ): Future[Unit] =
    if (existingLedgerId == providedLedgerId) {
      logger.info(s"Found existing ledger with ID: $existingLedgerId")
      Future.unit
    } else {
      Future.failed(new LedgerIdMismatchException(existingLedgerId, providedLedgerId))
    }

  private def initializeLedgerData(
      providedLedgerId: domain.LedgerId,
      ledgerDao: LedgerDao,
  ): Future[Unit] = {
    logger.info(s"Initializing ledger with ID: $providedLedgerId")
    ledgerDao.initializeLedger(providedLedgerId, Offset.begin)
  }
}

/**
  * @param startExclusive The last offset received from the read service.
  */
class JdbcIndexer private[indexer] (
    startExclusive: Option[Offset],
    participantId: ParticipantId,
    ledgerDao: LedgerDao,
    metrics: Metrics,
)(implicit mat: Materializer)
    extends Indexer {

  @volatile
  private var lastReceivedRecordTime: Long = Instant.now().toEpochMilli

  override def subscription(readService: ReadService): ResourceOwner[IndexFeedHandle] =
    new SubscriptionResourceOwner(readService)

  private def handleStateUpdate(offset: Offset, update: Update): Future[Unit] = {
    lastReceivedRecordTime = update.recordTime.toInstant.toEpochMilli

    metrics.daml.indexer.lastReceivedRecordTime.updateValue(lastReceivedRecordTime)
    metrics.daml.indexer.lastReceivedOffset.updateValue(offset.toApiString)

    val result = update match {
      case PartyAddedToParticipant(
          party,
          displayName,
          hostingParticipantId,
          recordTime,
          submissionId) =>
        ledgerDao
          .storePartyEntry(
            offset,
            PartyLedgerEntry.AllocationAccepted(
              submissionId,
              hostingParticipantId,
              recordTime.toInstant,
              domain
                .PartyDetails(party, Some(displayName), this.participantId == hostingParticipantId))
          )

      case PartyAllocationRejected(
          submissionId,
          hostingParticipantId,
          recordTime,
          rejectionReason) =>
        ledgerDao
          .storePartyEntry(
            offset,
            PartyLedgerEntry.AllocationRejected(
              submissionId,
              hostingParticipantId,
              recordTime.toInstant,
              rejectionReason
            )
          )

      case PublicPackageUpload(archives, optSourceDescription, recordTime, optSubmissionId) =>
        val recordTimeInstant = recordTime.toInstant
        val packages: List[(DamlLf.Archive, v2.PackageDetails)] = archives.map(
          archive =>
            archive -> v2.PackageDetails(
              size = archive.getPayload.size.toLong,
              knownSince = recordTimeInstant,
              sourceDescription = optSourceDescription))
        val optEntry: Option[PackageLedgerEntry] =
          optSubmissionId.map(submissionId =>
            PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTimeInstant))
        ledgerDao
          .storePackageEntry(
            offset,
            packages,
            optEntry
          )

      case PublicPackageUploadRejected(submissionId, recordTime, rejectionReason) =>
        val entry: PackageLedgerEntry =
          PackageLedgerEntry.PackageUploadRejected(
            submissionId,
            recordTime.toInstant,
            rejectionReason
          )
        ledgerDao
          .storePackageEntry(
            offset,
            List.empty,
            Some(entry)
          )

      case TransactionAccepted(
          optSubmitterInfo,
          transactionMeta,
          transaction,
          transactionId,
          recordTime,
          divulgedContracts) =>
        ledgerDao.storeTransaction(
          submitterInfo = optSubmitterInfo,
          workflowId = transactionMeta.workflowId,
          transactionId = transactionId,
          recordTime = recordTime.toInstant,
          ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
          offset = offset,
          transaction = transaction,
          divulged = divulgedContracts,
        )

      case config: ConfigurationChanged =>
        ledgerDao
          .storeConfigurationEntry(
            offset,
            config.recordTime.toInstant,
            config.submissionId,
            config.participantId,
            config.newConfiguration,
            None
          )

      case configRejection: ConfigurationChangeRejected =>
        ledgerDao
          .storeConfigurationEntry(
            offset,
            configRejection.recordTime.toInstant,
            configRejection.submissionId,
            configRejection.participantId,
            configRejection.proposedConfiguration,
            Some(configRejection.rejectionReason)
          )

      case CommandRejected(recordTime, submitterInfo, reason) =>
        ledgerDao.storeRejection(Some(submitterInfo), recordTime.toInstant, offset, reason)
    }
    result.map(_ => ())(DEC)
  }

  private class SubscriptionResourceOwner(readService: ReadService)
      extends ResourceOwner[IndexFeedHandle] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[IndexFeedHandle] =
      Resource(Future {
        metrics.daml.indexer.currentRecordTimeLag(() =>
          Instant.now().toEpochMilli - lastReceivedRecordTime)

        val (killSwitch, completionFuture) = readService
          .stateUpdates(startExclusive)
          .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
          .mapAsync(1) {
            case (offset, update) =>
              Timed
                .future(
                  metrics.daml.indexer.stateUpdateProcessing,
                  handleStateUpdate(offset, update))
          }
          .toMat(Sink.ignore)(Keep.both)
          .run()

        new SubscriptionIndexFeedHandle(killSwitch, completionFuture.map(_ => ()))
      })(
        handle =>
          for {
            _ <- Future(handle.killSwitch.shutdown())
            _ <- handle.completed.recover { case NonFatal(_) => () }
          } yield ()
      )
  }

  private class SubscriptionIndexFeedHandle(
      val killSwitch: KillSwitch,
      override val completed: Future[Unit],
  ) extends IndexFeedHandle
}
