// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.Instant

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink}
import com.codahale.metrics.{Gauge, MetricRegistry, Timer}
import com.daml.daml_lf_dev.DamlLf
import com.daml.dec.{DirectExecutionContext => DEC}
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Ref.LedgerString
import com.daml.lf.engine.Blinding
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{MetricName, Timed}
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.common.LedgerIdMismatchException
import com.daml.platform.configuration.ServerRole
import com.daml.platform.events.EventIdFormatter
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerDao}
import com.daml.platform.store.entries.{LedgerEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.{FlywayMigrations, PersistenceEntry}
import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final class JdbcIndexerFactory(
    serverRole: ServerRole,
    config: IndexerConfig,
    readService: ReadService,
    metrics: MetricRegistry,
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
    metrics: MetricRegistry,
)(implicit mat: Materializer)
    extends Indexer {

  @volatile
  private var lastReceivedRecordTime: Long = Instant.now().toEpochMilli

  @volatile
  private var lastReceivedOffset: LedgerString = _

  object Metrics {
    private val prefix = MetricName.DAML :+ "indexer"

    private val processedStateUpdatesName = prefix :+ "processed_state_updates"
    private val lastReceivedRecordTimeName = prefix :+ "last_received_record_time"
    private val lastReceivedOffsetName = prefix :+ "last_received_offset"
    private val currentRecordTimeLagName = prefix :+ "current_record_time_lag"

    val stateUpdateProcessingTimer: Timer = metrics.timer(processedStateUpdatesName)

    private[JdbcIndexer] def setup(): Unit = {

      metrics.remove(lastReceivedRecordTimeName)
      metrics.remove(lastReceivedOffsetName)
      metrics.remove(currentRecordTimeLagName)

      metrics.gauge(
        lastReceivedRecordTimeName,
        () =>
          new Gauge[Long] {
            override def getValue: Long = lastReceivedRecordTime
        })

      metrics.gauge(
        lastReceivedOffsetName,
        () =>
          new Gauge[LedgerString] {
            override def getValue: LedgerString = lastReceivedOffset
        })

      metrics.gauge(
        currentRecordTimeLagName,
        () =>
          new Gauge[Long] {
            override def getValue: Long =
              Instant.now().toEpochMilli - lastReceivedRecordTime
        })
      ()
    }
  }

  override def subscription(readService: ReadService): ResourceOwner[IndexFeedHandle] =
    new SubscriptionResourceOwner(readService)

  private def handleStateUpdate(offset: Offset, update: Update): Future[Unit] = {
    lastReceivedOffset = offset.toApiString
    lastReceivedRecordTime = update.recordTime.toInstant.toEpochMilli

    val externalOffset = offset
    val result = update match {
      case PartyAddedToParticipant(
          party,
          displayName,
          hostingParticipantId,
          recordTime,
          submissionId) =>
        ledgerDao
          .storePartyEntry(
            externalOffset,
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
            externalOffset,
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
            externalOffset,
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
            externalOffset,
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
        val blindingInfo = Blinding.blind(transaction)

        val mappedDisclosure = blindingInfo.disclosure.map {
          case (nodeId, parties) =>
            EventIdFormatter.fromTransactionId(transactionId, nodeId) -> parties
        }

        // local blinding info only contains values on transactions with relative contractIds.
        // this does not happen here (see type of transaction: GenTransaction.WithTxValue[NodeId, Value.AbsoluteContractId])
        assert(blindingInfo.localDivulgence.isEmpty)

        val pt = PersistenceEntry.Transaction(
          LedgerEntry.Transaction(
            optSubmitterInfo.map(_.commandId),
            transactionId,
            optSubmitterInfo.map(_.applicationId),
            optSubmitterInfo.map(_.submitter),
            transactionMeta.workflowId,
            transactionMeta.ledgerEffectiveTime.toInstant,
            recordTime.toInstant,
            transaction
              .mapNodeId(EventIdFormatter.fromTransactionId(transactionId, _)),
            mappedDisclosure
          ),
          blindingInfo.globalDivulgence,
          divulgedContracts.map(c => c.contractId -> c.contractInst)
        )
        ledgerDao
          .storeLedgerEntry(externalOffset, pt)

      case config: ConfigurationChanged =>
        ledgerDao
          .storeConfigurationEntry(
            externalOffset,
            config.recordTime.toInstant,
            config.submissionId,
            config.participantId,
            config.newConfiguration,
            None
          )

      case configRejection: ConfigurationChangeRejected =>
        ledgerDao
          .storeConfigurationEntry(
            externalOffset,
            configRejection.recordTime.toInstant,
            configRejection.submissionId,
            configRejection.participantId,
            configRejection.proposedConfiguration,
            Some(configRejection.rejectionReason)
          )

      case CommandRejected(recordTime, submitterInfo, reason) =>
        val rejection = PersistenceEntry.Rejection(
          LedgerEntry.Rejection(
            recordTime.toInstant,
            submitterInfo.commandId,
            submitterInfo.applicationId,
            submitterInfo.submitter,
            toDomainRejection(submitterInfo, reason)
          )
        )
        ledgerDao
          .storeLedgerEntry(externalOffset, rejection)
    }
    result.map(_ => ())(DEC)
  }

  private def toDomainRejection(
      submitterInfo: SubmitterInfo,
      state: RejectionReason): domain.RejectionReason = state match {
    case RejectionReason.Inconsistent(_) => domain.RejectionReason.Inconsistent(state.description)
    case RejectionReason.Disputed(_) => domain.RejectionReason.Disputed(state.description)
    case RejectionReason.ResourcesExhausted => domain.RejectionReason.OutOfQuota(state.description)
    case RejectionReason.PartyNotKnownOnLedger =>
      domain.RejectionReason.PartyNotKnownOnLedger(state.description)
    case RejectionReason.SubmitterCannotActViaParticipant(_) =>
      domain.RejectionReason.SubmitterCannotActViaParticipant(state.description)
    case RejectionReason.InvalidLedgerTime(_) =>
      domain.RejectionReason.InvalidLedgerTime(state.description)
  }

  private class SubscriptionResourceOwner(readService: ReadService)
      extends ResourceOwner[IndexFeedHandle] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[IndexFeedHandle] =
      Resource(Future {
        Metrics.setup()

        val (killSwitch, completionFuture) = readService
          .stateUpdates(startExclusive)
          .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
          .mapAsync(1) {
            case (offset, update) =>
              Timed.future(Metrics.stateUpdateProcessingTimer, handleStateUpdate(offset, update))
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
