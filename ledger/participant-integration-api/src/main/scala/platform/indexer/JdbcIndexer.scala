// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.ParticipantId
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.common
import com.daml.platform.common.MismatchException
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.FlywayMigrations
import com.daml.platform.store.dao.events.LfValueTranslation
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerDao, PersistenceResponse}
import com.daml.platform.store.entries.{PackageLedgerEntry, PartyLedgerEntry}

import scala.concurrent.Future
import scala.util.control.NonFatal

object JdbcIndexer {

  private[daml] final class Factory(
      serverRole: ServerRole,
      config: IndexerConfig,
      readService: ReadService,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslation.Cache,
  )(implicit materializer: Materializer, loggingContext: LoggingContext) {

    private val logger = ContextualizedLogger.get(this.getClass)

    def validateSchema()(
        implicit resourceContext: ResourceContext): Future[ResourceOwner[JdbcIndexer]] =
      new FlywayMigrations(config.jdbcUrl)
        .validate()
        .map(_ => initialized())(resourceContext.executionContext)

    def migrateSchema(
        allowExistingSchema: Boolean,
    )(implicit resourceContext: ResourceContext): Future[ResourceOwner[JdbcIndexer]] =
      new FlywayMigrations(config.jdbcUrl)
        .migrate(allowExistingSchema)
        .map(_ => initialized())(resourceContext.executionContext)

    def resetSchema(): Future[ResourceOwner[JdbcIndexer]] =
      Future.successful(for {
        ledgerDao <- JdbcLedgerDao.writeOwner(
          serverRole,
          config.jdbcUrl,
          config.eventsPageSize,
          metrics,
          lfValueTranslationCache,
          jdbcAsyncCommits = true,
        )
        _ <- ResourceOwner.forFuture(() => ledgerDao.reset())
        initialLedgerEnd <- initializeLedger(ledgerDao)
      } yield new JdbcIndexer(initialLedgerEnd, config.participantId, ledgerDao, metrics))

    private def initialized(): ResourceOwner[JdbcIndexer] =
      for {
        ledgerDao <- JdbcLedgerDao.writeOwner(
          serverRole,
          config.jdbcUrl,
          config.eventsPageSize,
          metrics,
          lfValueTranslationCache,
          jdbcAsyncCommits = true,
        )
        initialLedgerEnd <- initializeLedger(ledgerDao)
      } yield new JdbcIndexer(initialLedgerEnd, config.participantId, ledgerDao, metrics)

    private def initializeLedger(dao: LedgerDao)(): ResourceOwner[Option[Offset]] =
      new ResourceOwner[Option[Offset]] {
        override def acquire()(implicit context: ResourceContext): Resource[Option[Offset]] =
          Resource.fromFuture(for {
            initialConditions <- readService.getLedgerInitialConditions().runWith(Sink.head)
            existingLedgerId <- dao.lookupLedgerId()
            providedLedgerId = domain.LedgerId(initialConditions.ledgerId)
            _ <- existingLedgerId.fold(initializeLedgerData(providedLedgerId, dao))(
              checkLedgerIds(_, providedLedgerId))
            _ <- initOrCheckParticipantId(dao)
            initialLedgerEnd <- dao.lookupInitialLedgerEnd()
          } yield initialLedgerEnd)
      }

    private def checkLedgerIds(
        existingLedgerId: domain.LedgerId,
        providedLedgerId: domain.LedgerId,
    ): Future[Unit] =
      if (existingLedgerId == providedLedgerId) {
        logger.info(s"Found existing ledger with ID: $existingLedgerId")
        Future.unit
      } else {
        Future.failed(new MismatchException.LedgerId(existingLedgerId, providedLedgerId))
      }

    private def initializeLedgerData(
        providedLedgerId: domain.LedgerId,
        ledgerDao: LedgerDao,
    ): Future[Unit] = {
      logger.info(s"Initializing ledger with ID: $providedLedgerId")
      ledgerDao.initializeLedger(providedLedgerId)
    }

    private def initOrCheckParticipantId(
        dao: LedgerDao,
    )(implicit resourceContext: ResourceContext): Future[Unit] = {
      val id = ParticipantId(Ref.ParticipantId.assertFromString(config.participantId))
      dao
        .lookupParticipantId()
        .flatMap(
          _.fold(dao.initializeParticipantId(id)) {
            case `id` =>
              Future.successful(logger.info(s"Found existing participant id '$id'"))
            case retrievedLedgerId =>
              Future.failed(new common.MismatchException.ParticipantId(retrievedLedgerId, id))
          }
        )(resourceContext.executionContext)
    }

  }

  private def loggingContextPartiesValue(parties: List[Party]) =
    parties.mkString("[", ", ", "]")

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
          "updateSourceDescription" -> sourceDescription.getOrElse("")
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
          .map(
            info =>
              Map(
                "updateSubmitter" -> loggingContextPartiesValue(info.actAs),
                "updateApplicationId" -> info.applicationId,
                "updateCommandId" -> info.commandId,
                "updateDeduplicateUntil" -> info.deduplicateUntil.toString,
            ))
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

  private def loggingContextFor(offset: Offset, update: Update): Map[String, String] =
    loggingContextFor(update)
      .updated("updateRecordTime", update.recordTime.toInstant.toString)
      .updated("updateOffset", offset.toHexString)

  private val logger = ContextualizedLogger.get(classOf[JdbcIndexer])

}

/**
  * @param startExclusive The last offset received from the read service.
  */
private[daml] class JdbcIndexer private[indexer] (
    startExclusive: Option[Offset],
    participantId: v1.ParticipantId,
    ledgerDao: LedgerDao,
    metrics: Metrics,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends Indexer {

  import JdbcIndexer.logger

  override def subscription(readService: ReadService): ResourceOwner[IndexFeedHandle] =
    new SubscriptionResourceOwner(readService)

  private def handleStateUpdate(
      implicit loggingContext: LoggingContext): Flow[(Offset, Update), Unit, NotUsed] =
    Flow[(Offset, Update)]
      .wireTap(Sink.foreach[(Offset, Update)] {
        case (offset, update) =>
          val lastReceivedRecordTime = update.recordTime.toInstant.toEpochMilli

          logger.trace(update.description)

          metrics.daml.indexer.lastReceivedRecordTime.updateValue(lastReceivedRecordTime)
          metrics.daml.indexer.lastReceivedOffset.updateValue(offset.toApiString)
      })
      .mapAsync(1)((prepareTransactionInsert _).tupled)
      .mapAsync(1) {
        case kvUpdate @ OffsetUpdate(offset, update) =>
          withEnrichedLoggingContext(JdbcIndexer.loggingContextFor(offset, update)) {
            implicit loggingContext =>
              Timed.future(
                metrics.daml.indexer.stateUpdateProcessing,
                executeUpdate(kvUpdate),
              )
          }
      }
      .map(_ => ())

  private def prepareTransactionInsert(offset: Offset, update: Update): Future[OffsetUpdate] =
    update match {
      case tx: TransactionAccepted =>
        Timed.future(
          metrics.daml.index.db.storeTransactionDbMetrics.prepareBatches,
          Future {
            OffsetUpdate.PreparedTransactionInsert(
              offset = offset,
              update = tx,
              preparedInsert = ledgerDao.prepareTransactionInsert(
                submitterInfo = tx.optSubmitterInfo,
                workflowId = tx.transactionMeta.workflowId,
                transactionId = tx.transactionId,
                ledgerEffectiveTime = tx.transactionMeta.ledgerEffectiveTime.toInstant,
                offset = offset,
                transaction = tx.transaction,
                divulgedContracts = tx.divulgedContracts,
                blindingInfo = tx.blindingInfo,
              )
            )
          }(mat.executionContext)
        )
      case update => Future.successful(OffsetUpdate.OffsetUpdatePair(offset, update))
    }

  private def executeUpdate(offsetUpdate: OffsetUpdate)(
      implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    offsetUpdate match {
      case OffsetUpdate.PreparedTransactionInsert(
          offset,
          TransactionAccepted(
            optSubmitterInfo,
            transactionMeta,
            transaction,
            transactionId,
            recordTime,
            divulgedContracts,
            blindingInfo,
          ),
          preparedInsert) =>
        ledgerDao.storeTransaction(
          preparedInsert,
          submitterInfo = optSubmitterInfo,
          transactionId = transactionId,
          recordTime = recordTime.toInstant,
          ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
          offset = offset,
          transaction = transaction,
          divulged = divulgedContracts,
          blindingInfo = blindingInfo,
        )
      case OffsetUpdate.OffsetUpdatePair(offset, update) =>
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
              domain.PartyDetails(party, Some(displayName), participantId == hostingParticipantId)
            )
            ledgerDao.storePartyEntry(offset, entry)

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
            ledgerDao.storePartyEntry(offset, entry)

          case PublicPackageUpload(archives, optSourceDescription, recordTime, optSubmissionId) =>
            val recordTimeInstant = recordTime.toInstant
            val packages: List[(DamlLf.Archive, v2.PackageDetails)] = archives.map(
              archive =>
                archive -> v2.PackageDetails(
                  size = archive.getPayload.size.toLong,
                  knownSince = recordTimeInstant,
                  sourceDescription = optSourceDescription,
              ))
            val optEntry: Option[PackageLedgerEntry] =
              optSubmissionId.map(submissionId =>
                PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTimeInstant))
            ledgerDao.storePackageEntry(offset, packages, optEntry)

          case PublicPackageUploadRejected(submissionId, recordTime, rejectionReason) =>
            val entry = PackageLedgerEntry.PackageUploadRejected(
              submissionId,
              recordTime.toInstant,
              rejectionReason,
            )
            ledgerDao.storePackageEntry(offset, List.empty, Some(entry))

          case config: ConfigurationChanged =>
            ledgerDao.storeConfigurationEntry(
              offset,
              config.recordTime.toInstant,
              config.submissionId,
              config.newConfiguration,
              None,
            )

          case configRejection: ConfigurationChangeRejected =>
            ledgerDao.storeConfigurationEntry(
              offset,
              configRejection.recordTime.toInstant,
              configRejection.submissionId,
              configRejection.proposedConfiguration,
              Some(configRejection.rejectionReason),
            )

          case CommandRejected(recordTime, submitterInfo, reason) =>
            ledgerDao.storeRejection(Some(submitterInfo), recordTime.toInstant, offset, reason)
          case update: TransactionAccepted =>
            import update._
            logger.warn(
              """For performance considerations, TransactionAccepted should be handled in a different branch.
                |Recomputing PreparedInsert..""".stripMargin)
            ledgerDao.storeTransaction(
              preparedInsert = ledgerDao.prepareTransactionInsert(
                submitterInfo = optSubmitterInfo,
                workflowId = transactionMeta.workflowId,
                transactionId = transactionId,
                ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
                offset = offset,
                transaction = transaction,
                divulgedContracts = divulgedContracts,
                blindingInfo = blindingInfo,
              ),
              submitterInfo = optSubmitterInfo,
              transactionId = transactionId,
              recordTime = recordTime.toInstant,
              ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
              offset = offset,
              transaction = transaction,
              divulged = divulgedContracts,
              blindingInfo = blindingInfo,
            )
        }
    }

  private class SubscriptionResourceOwner(
      readService: ReadService,
  )(implicit loggingContext: LoggingContext)
      extends ResourceOwner[IndexFeedHandle] {
    override def acquire()(implicit context: ResourceContext): Resource[IndexFeedHandle] =
      Resource(Future {
        val (killSwitch, completionFuture) = readService
          .stateUpdates(startExclusive)
          .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
          .via(handleStateUpdate)
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
