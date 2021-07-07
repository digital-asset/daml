// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.appendonlydao

import java.sql.Connection
import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{LedgerId, ParticipantId, PartyDetails}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationDuplicate,
  CommandDeduplicationNew,
  CommandDeduplicationResult,
  PackageDetails,
}
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.lf.archive.Reader
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.engine.ValueEnricher
import com.daml.lf.transaction.BlindingInfo
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.entries.LoggingEntry
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.{CurrentOffset, IncrementalOffsetStep, OffsetStep}
import com.daml.platform.store.Conversions._
import com.daml.platform.store._
import com.daml.platform.store.appendonlydao.events.{
  CompressionStrategy,
  ContractsReader,
  LfValueTranslation,
  PostCommitValidation,
  QueryNonPrunedImpl,
  TransactionsReader,
}
import com.daml.platform.store.backend.{StorageBackend, UpdateToDbDto}
import com.daml.platform.store.dao.ParametersTable.LedgerEndUpdateError
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert
import com.daml.platform.store.dao.{
  DeduplicationKeyMaker,
  LedgerDao,
  LedgerReadDao,
  MeteredLedgerDao,
  MeteredLedgerReadDao,
  PersistenceResponse,
}
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry,
}
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

private class JdbcLedgerDao(
    dbDispatcher: DbDispatcher,
    servicesExecutionContext: ExecutionContext,
    eventsPageSize: Int,
    eventsProcessingParallelism: Int,
    performPostCommitValidation: Boolean,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslationCache.Cache,
    validatePartyAllocation: Boolean,
    enricher: Option[ValueEnricher],
    sequentialIndexer: SequentialWriteDao,
    participantId: v1.ParticipantId,
    storageBackend: StorageBackend[_],
) extends LedgerDao {

  import JdbcLedgerDao._

  private val logger = ContextualizedLogger.get(this.getClass)

  override def currentHealth(): HealthStatus = dbDispatcher.currentHealth()

  override def lookupLedgerId()(implicit loggingContext: LoggingContext): Future[Option[LedgerId]] =
    dbDispatcher.executeSql(metrics.daml.index.db.getLedgerId)(storageBackend.ledgerId)

  override def lookupParticipantId()(implicit
      loggingContext: LoggingContext
  ): Future[Option[ParticipantId]] =
    dbDispatcher.executeSql(metrics.daml.index.db.getParticipantId)(storageBackend.participantId)

  /** Defaults to Offset.begin if ledger_end is unset
    */
  override def lookupLedgerEnd()(implicit loggingContext: LoggingContext): Future[Offset] =
    dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEnd)(storageBackend.ledgerEndOffset)

  override def lookupLedgerEndOffsetAndSequentialId()(implicit
      loggingContext: LoggingContext
  ): Future[(Offset, Long)] =
    dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEndOffsetAndSequentialId)(
      storageBackend.ledgerEndOffsetAndSequentialId
    )

  override def lookupInitialLedgerEnd()(implicit
      loggingContext: LoggingContext
  ): Future[Option[Offset]] =
    dbDispatcher.executeSql(metrics.daml.index.db.getInitialLedgerEnd)(
      storageBackend.initialLedgerEnd
    )

  override def initializeLedger(
      ledgerId: LedgerId
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.initializeLedgerParameters) {
      implicit connection =>
        storageBackend.enforceSynchronousCommit(connection)
        storageBackend.updateLedgerId(ledgerId.unwrap)(connection)
    }

  override def initializeParticipantId(
      participantId: ParticipantId
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.initializeParticipantId) { implicit connection =>
      storageBackend.enforceSynchronousCommit(connection)
      storageBackend.updateParticipantId(participantId.unwrap)(connection)
    }

  override def lookupLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Offset, Configuration)]] =
    dbDispatcher.executeSql(metrics.daml.index.db.lookupConfiguration)(
      storageBackend.ledgerConfiguration
    )

  override def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, ConfigurationEntry), NotUsed] =
    PaginatingAsyncStream(PageSize) { queryOffset =>
      withEnrichedLoggingContext("queryOffset" -> queryOffset) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.index.db.loadConfigurationEntries) {
          storageBackend.configurationEntries(
            startExclusive = startExclusive,
            endInclusive = endInclusive,
            pageSize = PageSize,
            queryOffset = queryOffset,
          )
        }
      }
    }

  override def storeConfigurationEntry(
      offsetStep: OffsetStep,
      recordedAt: Instant,
      submissionId: String,
      configuration: Configuration,
      rejectionReason: Option[String],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    withEnrichedLoggingContext(Logging.submissionId(submissionId)) { implicit loggingContext =>
      logger.info("Storing configuration entry")
      dbDispatcher.executeSql(
        metrics.daml.index.db.storeConfigurationEntryDbMetrics
      ) { implicit conn =>
        val optCurrentConfig = storageBackend.ledgerConfiguration(conn)
        val optExpectedGeneration: Option[Long] =
          optCurrentConfig.map { case (_, c) => c.generation + 1 }
        val finalRejectionReason: Option[String] =
          optExpectedGeneration match {
            case Some(expGeneration)
                if rejectionReason.isEmpty && expGeneration != configuration.generation =>
              // If we're not storing a rejection and the new generation is not succ of current configuration, then
              // we store a rejection. This code path is only expected to be taken in sandbox. This follows the same
              // pattern as with transactions.
              Some(
                s"Generation mismatch: expected=$expGeneration, actual=${configuration.generation}"
              )

            case _ =>
              // Rejection reason was set, or we have no previous configuration generation, in which case we accept any
              // generation.
              rejectionReason
          }

        val update = finalRejectionReason match {
          case None =>
            Update.ConfigurationChanged(
              recordTime = Time.Timestamp.assertFromInstant(recordedAt),
              submissionId = SubmissionId.assertFromString(submissionId),
              participantId =
                v1.ParticipantId.assertFromString("1"), // not used for DbDto generation
              newConfiguration = configuration,
            )

          case Some(reason) =>
            Update.ConfigurationChangeRejected(
              recordTime = Time.Timestamp.assertFromInstant(recordedAt),
              submissionId = SubmissionId.assertFromString(submissionId),
              participantId =
                v1.ParticipantId.assertFromString("1"), // not used for DbDto generation
              proposedConfiguration = configuration,
              rejectionReason = reason,
            )
        }

        val savepoint = conn.setSavepoint()

        val offset = validateOffsetStep(offsetStep, conn)
        Try({
          sequentialIndexer.store(conn, offset, Some(update))
          PersistenceResponse.Ok
        }).recover {
          case NonFatal(e) if e.getMessage.contains(storageBackend.duplicateKeyError) =>
            logger.warn(s"Ignoring duplicate configuration submission, submissionId=$submissionId")
            conn.rollback(savepoint)
            sequentialIndexer.store(
              conn,
              offset,
              None,
            ) // we bump the offset regardless of the fact of a duplicate
            PersistenceResponse.Duplicate
        }.get
      }
    }

  override def storePartyEntry(
      offsetStep: OffsetStep,
      partyEntry: PartyLedgerEntry,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing party entry")
    dbDispatcher.executeSql(metrics.daml.index.db.storePartyEntryDbMetrics) { implicit conn =>
      val savepoint = conn.setSavepoint()
      val offset = validateOffsetStep(offsetStep, conn)
      partyEntry match {
        case PartyLedgerEntry.AllocationAccepted(submissionIdOpt, recordTime, partyDetails) =>
          Try({
            sequentialIndexer.store(
              conn,
              offset,
              Some(
                Update.PartyAddedToParticipant(
                  party = partyDetails.party,
                  displayName = partyDetails.displayName.orNull,
                  participantId = participantId,
                  recordTime = Time.Timestamp.assertFromInstant(recordTime),
                  submissionId = submissionIdOpt,
                )
              ),
            )
            PersistenceResponse.Ok
          }).recover {
            case NonFatal(e) if e.getMessage.contains(storageBackend.duplicateKeyError) =>
              logger.warn(
                s"Ignoring duplicate party submission with ID ${partyDetails.party} for submissionId $submissionIdOpt"
              )
              conn.rollback(savepoint)
              sequentialIndexer.store(conn, offset, None)
              PersistenceResponse.Duplicate
          }.get

        case PartyLedgerEntry.AllocationRejected(submissionId, recordTime, reason) =>
          sequentialIndexer.store(
            conn,
            offset,
            Some(
              Update.PartyAllocationRejected(
                submissionId = submissionId,
                participantId = participantId,
                recordTime = Time.Timestamp.assertFromInstant(recordTime),
                rejectionReason = reason,
              )
            ),
          )
          PersistenceResponse.Ok
      }
    }
  }

  override def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, PartyLedgerEntry), NotUsed] = {
    PaginatingAsyncStream(PageSize) { queryOffset =>
      withEnrichedLoggingContext("queryOffset" -> queryOffset) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.index.db.loadPartyEntries)(
          storageBackend.partyEntries(
            startExclusive = startExclusive,
            endInclusive = endInclusive,
            pageSize = PageSize,
            queryOffset = queryOffset,
          )
        )
      }
    }
  }

  override def prepareTransactionInsert(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  ): PreparedInsert =
    throw new UnsupportedOperationException(
      "not supported by append-only code"
    ) // TODO append-only: cleanup

  // TODO append-only: cleanup
  override def storeTransactionState(
      preparedInsert: PreparedInsert
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    throw new UnsupportedOperationException(
      "not supported by append-only code"
    ) // TODO append-only: cleanup

  override def storeTransactionEvents(
      preparedInsert: PreparedInsert
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    throw new UnsupportedOperationException(
      "not supported by append-only code"
    ) // TODO append-only: cleanup

  override def completeTransaction(
      submitterInfo: Option[SubmitterInfo],
      transactionId: TransactionId,
      recordTime: Instant,
      offsetStep: OffsetStep,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    throw new UnsupportedOperationException(
      "not supported by append-only code"
    ) // TODO append-only: cleanup

  override def storeTransaction(
      preparedInsert: PreparedInsert,
      submitterInfo: Option[SubmitterInfo],
      transactionId: TransactionId,
      recordTime: Instant,
      ledgerEffectiveTime: Instant,
      offsetStep: OffsetStep,
      transaction: CommittedTransaction,
      divulged: Iterable[DivulgedContract],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    throw new UnsupportedOperationException(
      "not supported by append-only code"
    ) // TODO append-only: cleanup

  private def validate(
      ledgerEffectiveTime: Instant,
      transaction: CommittedTransaction,
      divulged: Iterable[DivulgedContract],
  )(implicit connection: Connection): Option[RejectionReason] =
    Timed.value(
      metrics.daml.index.db.storeTransactionDbMetrics.commitValidation,
      postCommitValidation.validate(
        transaction = transaction,
        transactionLedgerEffectiveTime = ledgerEffectiveTime,
        divulged = divulged.iterator.map(_.contractId).toSet,
      ),
    )

  override def storeRejection(
      submitterInfo: Option[SubmitterInfo],
      recordTime: Instant,
      offsetStep: OffsetStep,
      reason: RejectionReason,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.storeRejectionDbMetrics) { implicit conn =>
        val offset = validateOffsetStep(offsetStep, conn)
        sequentialIndexer.store(
          conn,
          offset,
          submitterInfo.map(someSubmitterInfo =>
            Update.CommandRejected(
              recordTime = Time.Timestamp.assertFromInstant(recordTime),
              submitterInfo = someSubmitterInfo,
              reason = reason,
            )
          ),
        )
        PersistenceResponse.Ok
      }

  override def storeInitialState(
      ledgerEntries: Vector[(Offset, LedgerEntry)],
      newLedgerEnd: Offset,
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    logger.info("Storing initial state")
    dbDispatcher.executeSql(metrics.daml.index.db.storeInitialStateFromScenario) {
      implicit connection =>
        storageBackend.enforceSynchronousCommit(connection)
        ledgerEntries.foreach { case (offset, entry) =>
          entry match {
            case tx: LedgerEntry.Transaction =>
              val submitterInfo =
                for (
                  appId <- tx.applicationId;
                  actAs <- if (tx.actAs.isEmpty) None else Some(tx.actAs);
                  cmdId <- tx.commandId
                ) yield SubmitterInfo(actAs, appId, cmdId, Instant.EPOCH)

              sequentialIndexer.store(
                connection,
                offset,
                Some(
                  Update.TransactionAccepted(
                    optSubmitterInfo = submitterInfo,
                    transactionMeta = TransactionMeta(
                      ledgerEffectiveTime =
                        Time.Timestamp.assertFromInstant(tx.ledgerEffectiveTime),
                      workflowId = tx.workflowId,
                      submissionTime = null, // not used for DbDto generation
                      submissionSeed = null, // not used for DbDto generation
                      optUsedPackages = None, // not used for DbDto generation
                      optNodeSeeds = None, // not used for DbDto generation
                      optByKeyNodes = None, // not used for DbDto generation
                    ),
                    transaction = tx.transaction,
                    transactionId = tx.transactionId,
                    recordTime = Time.Timestamp.assertFromInstant(tx.recordedAt),
                    divulgedContracts = Nil,
                    blindingInfo = None,
                  )
                ),
              )
            case LedgerEntry.Rejection(recordTime, commandId, applicationId, actAs, reason) =>
              sequentialIndexer.store(
                connection,
                offset,
                Some(
                  Update.CommandRejected(
                    recordTime = Time.Timestamp.assertFromInstant(recordTime),
                    submitterInfo = SubmitterInfo(actAs, applicationId, commandId, Instant.EPOCH),
                    reason = reason,
                  )
                ),
              )
          }
        }
        sequentialIndexer.store(connection, newLedgerEnd, None)
    }
  }

  private val PageSize = 100

  override def getParties(
      parties: Seq[Party]
  )(implicit loggingContext: LoggingContext): Future[List[PartyDetails]] =
    if (parties.isEmpty)
      Future.successful(List.empty)
    else
      dbDispatcher
        .executeSql(metrics.daml.index.db.loadParties)(storageBackend.parties(parties))

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadAllParties)(storageBackend.knownParties)

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[PackageId, PackageDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadPackages)(storageBackend.lfPackages)

  override def getLfArchive(
      packageId: PackageId
  )(implicit loggingContext: LoggingContext): Future[Option[Archive]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadArchive)(storageBackend.lfArchive(packageId))
      .map(_.map(data => Archive.parseFrom(Reader.damlLfCodedInputStreamFromBytes(data))))(
        servicesExecutionContext
      )

  override def storePackageEntry(
      offsetStep: OffsetStep,
      packages: List[(Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing package entry")
    dbDispatcher.executeSql(metrics.daml.index.db.storePackageEntryDbMetrics) {
      implicit connection =>
        val offset = validateOffsetStep(offsetStep, connection)
        // Note on knownSince and recordTime:
        // - There are two different time values in the input: PackageDetails.knownSince and PackageUploadAccepted.recordTime
        // - There is only one time value in the intermediate values: PublicPackageUpload.recordTime
        // - There are two different time values in the database schema: packages.known_since and package_entries.recorded_at
        // This is not an issue since all callers of this method always use the same value for all knownSince and recordTime times.
        //
        // Note on sourceDescription:
        // - In the input, each package can have its own source description (see PackageDetails.sourceDescription)
        // - In the intermediate value, there is only one source description for all packages (see PublicPackageUpload.sourceDescription)
        // - In the database schema, each package can have its own source description (see packages.source_description)
        // This is again not an issue since all callers of this method always use the same value for all source descriptions.
        val update = optEntry match {
          case None =>
            // Calling storePackageEntry() without providing a PackageLedgerEntry is used to copy initial packages,
            // or in the case where the submission ID is unknown (package was submitted through a different participant).
            Update.PublicPackageUpload(
              archives = packages.view.map(_._1).toList,
              sourceDescription = packages.headOption.flatMap(
                _._2.sourceDescription
              ),
              recordTime = Time.Timestamp.assertFromInstant(
                packages.headOption
                  .map(
                    _._2.knownSince
                  )
                  .getOrElse(Instant.EPOCH)
              ),
              submissionId =
                None, // If the submission ID is missing, this update will not insert a row in the package_entries table
            )

          case Some(PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTime)) =>
            Update.PublicPackageUpload(
              archives = packages.view.map(_._1).toList,
              sourceDescription = packages.headOption.flatMap(
                _._2.sourceDescription
              ),
              recordTime = Time.Timestamp.assertFromInstant(recordTime),
              submissionId = Some(submissionId),
            )

          case Some(PackageLedgerEntry.PackageUploadRejected(submissionId, recordTime, reason)) =>
            Update.PublicPackageUploadRejected(
              submissionId = submissionId,
              recordTime = Time.Timestamp.assertFromInstant(recordTime),
              rejectionReason = reason,
            )
        }
        sequentialIndexer.store(connection, offset, Some(update))
        PersistenceResponse.Ok
    }
  }

  override def getPackageEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, PackageLedgerEntry), NotUsed] =
    PaginatingAsyncStream(PageSize) { queryOffset =>
      withEnrichedLoggingContext("queryOffset" -> queryOffset) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.index.db.loadPackageEntries)(
          storageBackend.packageEntries(
            startExclusive = startExclusive,
            endInclusive = endInclusive,
            pageSize = PageSize,
            queryOffset = queryOffset,
          )
        )
      }
    }

  override def deduplicateCommand(
      commandId: domain.CommandId,
      submitters: List[Ref.Party],
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult] =
    dbDispatcher.executeSql(metrics.daml.index.db.deduplicateCommandDbMetrics) { conn =>
      val key = DeduplicationKeyMaker.make(commandId, submitters)
      // Insert a new deduplication entry, or update an expired entry
      val updated = storageBackend.upsertDeduplicationEntry(
        key = key,
        submittedAt = submittedAt,
        deduplicateUntil = deduplicateUntil,
      )(conn)

      if (updated == 1) {
        // New row inserted, this is the first time the command is submitted
        CommandDeduplicationNew
      } else {
        // Deduplication row already exists
        CommandDeduplicationDuplicate(storageBackend.deduplicatedUntil(key)(conn))
      }
    }

  override def removeExpiredDeduplicationData(
      currentTime: Instant
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.removeExpiredDeduplicationDataDbMetrics)(
      storageBackend.removeExpiredDeduplicationData(currentTime)
    )

  override def stopDeduplicatingCommand(
      commandId: domain.CommandId,
      submitters: List[Party],
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    val key = DeduplicationKeyMaker.make(commandId, submitters)
    dbDispatcher.executeSql(metrics.daml.index.db.stopDeduplicatingCommandDbMetrics)(
      storageBackend.stopDeduplicatingCommand(key)
    )
  }

  override def prune(
      pruneUpToInclusive: Offset
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.pruneDbMetrics) { conn =>
      storageBackend.pruneEvents(pruneUpToInclusive)(conn)
      storageBackend.pruneCompletions(pruneUpToInclusive)(conn)
      storageBackend.updatePrunedUptoInclusive(pruneUpToInclusive)(conn)
      logger.info(s"Pruned ledger api server index db up to ${pruneUpToInclusive.toHexString}")
    }

  override def reset()(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.truncateAllTables)(storageBackend.reset)

  private val translation: LfValueTranslation =
    new LfValueTranslation(
      cache = lfValueTranslationCache,
      metrics = metrics,
      enricherO = enricher,
      loadPackage = (packageId, loggingContext) => this.getLfArchive(packageId)(loggingContext),
    )

  private val queryNonPruned = QueryNonPrunedImpl(storageBackend)

  override val transactionsReader: TransactionsReader =
    new TransactionsReader(
      dispatcher = dbDispatcher,
      queryNonPruned = queryNonPruned,
      storageBackend = storageBackend,
      pageSize = eventsPageSize,
      eventProcessingParallelism = eventsProcessingParallelism,
      metrics = metrics,
      lfValueTranslation = translation,
    )(
      servicesExecutionContext
    )

  override val contractsReader: ContractsReader =
    ContractsReader(dbDispatcher, metrics, storageBackend)(
      servicesExecutionContext
    )

  override val completions: CommandCompletionsReader =
    new CommandCompletionsReader(
      dbDispatcher,
      storageBackend,
      queryNonPruned,
      metrics,
      servicesExecutionContext,
    )

  private val postCommitValidation =
    if (performPostCommitValidation)
      new PostCommitValidation.BackedBy(storageBackend, validatePartyAllocation)
    else
      PostCommitValidation.Skip

  /** This is a combined store transaction method to support sandbox-classic and tests
    * !!! Usage of this is discouraged, with the removal of sandbox-classic this will be removed
    */
  override def storeTransaction(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      offsetStep: OffsetStep,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
      recordTime: Instant,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing transaction")
    dbDispatcher
      .executeSql(metrics.daml.index.db.storeTransactionDbMetrics) { implicit conn =>
        sequentialIndexer.store(
          conn,
          validateOffsetStep(offsetStep, conn),
          validate(ledgerEffectiveTime, transaction, divulgedContracts) match {
            case None =>
              Some(
                Update.TransactionAccepted(
                  optSubmitterInfo = submitterInfo,
                  transactionMeta = TransactionMeta(
                    ledgerEffectiveTime = Time.Timestamp.assertFromInstant(ledgerEffectiveTime),
                    workflowId = workflowId,
                    submissionTime = null, // not used for DbDto generation
                    submissionSeed = null, // not used for DbDto generation
                    optUsedPackages = None, // not used for DbDto generation
                    optNodeSeeds = None, // not used for DbDto generation
                    optByKeyNodes = None, // not used for DbDto generation
                  ),
                  transaction = transaction,
                  transactionId = transactionId,
                  recordTime = Time.Timestamp.assertFromInstant(recordTime),
                  divulgedContracts = divulgedContracts.toList,
                  blindingInfo = blindingInfo,
                )
              )

            case Some(error) =>
              submitterInfo.map(someSubmitterInfo =>
                Update.CommandRejected(
                  recordTime = Time.Timestamp.assertFromInstant(recordTime),
                  submitterInfo = someSubmitterInfo,
                  reason = error,
                )
              )
          },
        )
        PersistenceResponse.Ok
      }
  }

  // The old indexer (com.daml.platform.indexer.JdbcIndexer) uses IncrementalOffsetStep,
  // and we have tests in JdbcLedgerDao*Spec checking that this is handled correctly.
  // The append-only schema and the parallel ingestion indexer doesn't use this class.
  // TODO append-only: Remove the OffsetStep trait along with this method and all corresponding tests,
  // and change all method signatures to use Offset instead of OffsetStep.
  private[this] def validateOffsetStep(offsetStep: OffsetStep, conn: Connection): Offset = {
    offsetStep match {
      case IncrementalOffsetStep(p, o) =>
        val actualEnd = storageBackend.ledgerEndOffset(conn)
        if (actualEnd.compareTo(p) != 0) throw LedgerEndUpdateError(p) else o
      case CurrentOffset(o) => o
    }
  }

}

private[platform] object JdbcLedgerDao {

  object Logging {
    def submissionId(id: String): LoggingEntry =
      "submissionId" -> id

    def transactionId(id: TransactionId): LoggingEntry =
      "transactionId" -> id
  }

  def readOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      connectionPoolSize: Int,
      connectionTimeout: FiniteDuration,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      enricher: Option[ValueEnricher],
      participantId: v1.ParticipantId,
  )(implicit loggingContext: LoggingContext): ResourceOwner[LedgerReadDao] = {
    owner(
      serverRole,
      jdbcUrl,
      connectionPoolSize,
      connectionTimeout,
      eventsPageSize,
      eventsProcessingParallelism,
      validate = false,
      servicesExecutionContext,
      metrics,
      lfValueTranslationCache,
      enricher = enricher,
      participantId = participantId,
      compressionStrategy = CompressionStrategy.none(metrics), // not needed
    ).map(new MeteredLedgerReadDao(_, metrics))
  }

  def writeOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      connectionPoolSize: Int,
      connectionTimeout: FiniteDuration,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      jdbcAsyncCommitMode: DbType.AsyncCommitMode,
      enricher: Option[ValueEnricher],
      participantId: v1.ParticipantId,
  )(implicit loggingContext: LoggingContext): ResourceOwner[LedgerDao] = {
    val dbType = DbType.jdbcType(jdbcUrl)
    owner(
      serverRole,
      jdbcUrl,
      dbType.maxSupportedWriteConnections(connectionPoolSize),
      connectionTimeout,
      eventsPageSize,
      eventsProcessingParallelism,
      validate = false,
      servicesExecutionContext,
      metrics,
      lfValueTranslationCache,
      jdbcAsyncCommitMode =
        if (dbType.supportsAsynchronousCommits) jdbcAsyncCommitMode else DbType.SynchronousCommit,
      enricher = enricher,
      participantId = participantId,
      compressionStrategy = CompressionStrategy.none(metrics), // not needed
    ).map(new MeteredLedgerDao(_, metrics))
  }

  def validatingWriteOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      connectionPoolSize: Int,
      connectionTimeout: FiniteDuration,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      validatePartyAllocation: Boolean = false,
      enricher: Option[ValueEnricher],
      participantId: v1.ParticipantId,
      compressionStrategy: CompressionStrategy,
  )(implicit loggingContext: LoggingContext): ResourceOwner[LedgerDao] = {
    val dbType = DbType.jdbcType(jdbcUrl)
    owner(
      serverRole,
      jdbcUrl,
      dbType.maxSupportedWriteConnections(connectionPoolSize),
      connectionTimeout,
      eventsPageSize,
      eventsProcessingParallelism,
      validate = true,
      servicesExecutionContext,
      metrics,
      lfValueTranslationCache,
      validatePartyAllocation,
      enricher = enricher,
      participantId = participantId,
      compressionStrategy = compressionStrategy,
    ).map(new MeteredLedgerDao(_, metrics))
  }

  private def sequentialWriteDao(
      participantId: v1.ParticipantId,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      metrics: Metrics,
      compressionStrategy: CompressionStrategy,
      storageBackend: StorageBackend[_],
  ): SequentialWriteDao =
    SequentialWriteDaoImpl(
      storageBackend = storageBackend,
      updateToDbDtos = UpdateToDbDto(
        participantId = participantId,
        translation = new LfValueTranslation(
          cache = lfValueTranslationCache,
          metrics = metrics,
          enricherO = None,
          loadPackage = (_, _) => Future.successful(None),
        ),
        compressionStrategy = compressionStrategy,
      ),
    )

  private def owner(
      serverRole: ServerRole,
      jdbcUrl: String,
      connectionPoolSize: Int,
      connectionTimeout: FiniteDuration,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      validate: Boolean,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      validatePartyAllocation: Boolean = false,
      jdbcAsyncCommitMode: DbType.AsyncCommitMode = DbType.SynchronousCommit,
      enricher: Option[ValueEnricher],
      participantId: v1.ParticipantId,
      compressionStrategy: CompressionStrategy,
  )(implicit loggingContext: LoggingContext): ResourceOwner[LedgerDao] =
    for {
      dbDispatcher <- DbDispatcher.owner(
        serverRole,
        jdbcUrl,
        connectionPoolSize,
        connectionTimeout,
        metrics,
        jdbcAsyncCommitMode,
      )
      dbType = DbType.jdbcType(jdbcUrl)
      storageBackend = StorageBackend.of(dbType)
    } yield new JdbcLedgerDao(
      dbDispatcher,
      servicesExecutionContext,
      eventsPageSize,
      eventsProcessingParallelism,
      validate,
      metrics,
      lfValueTranslationCache,
      validatePartyAllocation,
      enricher,
      sequentialWriteDao(
        participantId,
        lfValueTranslationCache,
        metrics,
        compressionStrategy,
        storageBackend,
      ),
      participantId,
      storageBackend,
    )

  // TODO H2 support
//  object H2DatabaseQueries extends Queries {
//    override protected[JdbcLedgerDao] val SQL_INSERT_COMMAND: String =
//      """merge into participant_command_submissions pcs
//        |using dual on deduplication_key = {deduplicationKey}
//        |when not matched then
//        |  insert (deduplication_key, deduplicate_until)
//        |  values ({deduplicationKey}, {deduplicateUntil})
//        |when matched and pcs.deduplicate_until < {submittedAt} then
//        |  update set deduplicate_until={deduplicateUntil}""".stripMargin
//
//    override protected[JdbcLedgerDao] val DUPLICATE_KEY_ERROR: String =
//      "Unique index or primary key violation"
//
//    override protected[JdbcLedgerDao] val SQL_TRUNCATE_TABLES: String =
//      """set referential_integrity false;
//        |truncate table configuration_entries;
//        |truncate table package_entries;
//        |truncate table parameters;
//        |truncate table participant_command_completions;
//        |truncate table participant_command_submissions;
//        |truncate table participant_events;
//        |truncate table participant_contracts;
//        |truncate table participant_contract_witnesses;
//        |truncate table parties;
//        |truncate table party_entries;
//        |set referential_integrity true;
//      """.stripMargin
//
//    /** H2 does not support asynchronous commits */
//    override protected[JdbcLedgerDao] def enforceSynchronousCommit(implicit
//        conn: Connection
//    ): Unit = ()
//  }

  val acceptType = "accept"
  val rejectType = "reject"
}
