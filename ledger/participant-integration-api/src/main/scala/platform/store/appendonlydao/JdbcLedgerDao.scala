// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.appendonlydao

import java.sql.Connection
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{LedgerId, ParticipantId, PartyDetails}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationDuplicate,
  CommandDeduplicationNew,
  CommandDeduplicationResult,
  PackageDetails,
}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.archive.ArchiveParser
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.engine.ValueEnricher
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.entries.LoggingEntry
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.{CurrentOffset, IncrementalOffsetStep, OffsetStep}
import com.daml.platform.store.Conversions._
import com.daml.platform.store._
import com.daml.platform.store.appendonlydao.events._
import com.daml.platform.store.backend.{ParameterStorageBackend, StorageBackend, UpdateToDbDto}
import com.daml.platform.store.cache.StringInterningCache
import com.daml.platform.store.dao.{
  DeduplicationKeyMaker,
  LedgerDao,
  LedgerReadDao,
  MeteredLedgerDao,
  MeteredLedgerReadDao,
  PersistenceResponse,
}
import com.daml.platform.store.dao.ParametersTable.LedgerEndUpdateError
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry,
}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

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
    participantId: Ref.ParticipantId,
    storageBackend: StorageBackend[_],
    stringInterningCache: AtomicReference[StringInterningCache],
    ledgerEnd: AtomicReference[(Offset, Long)],
) extends LedgerDao {

  import JdbcLedgerDao._

  private val logger = ContextualizedLogger.get(this.getClass)

  override def currentHealth(): HealthStatus = dbDispatcher.currentHealth()

  override def lookupLedgerId()(implicit loggingContext: LoggingContext): Future[Option[LedgerId]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.getLedgerId)(
        storageBackend.ledgerIdentity(_).map(_.ledgerId)
      )

  override def lookupParticipantId()(implicit
      loggingContext: LoggingContext
  ): Future[Option[ParticipantId]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.getParticipantId)(
        storageBackend.ledgerIdentity(_).map(_.participantId)
      )

  /** Defaults to Offset.begin if ledger_end is unset
    */
  override def lookupLedgerEnd()(implicit loggingContext: LoggingContext): Future[Offset] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.getLedgerEnd)(
        storageBackend.ledgerEndOrBeforeBegin(_).lastOffset
      )

  case class InvalidLedgerEnd(msg: String) extends RuntimeException(msg)

  override def lookupLedgerEndOffsetAndSequentialId()(implicit
      loggingContext: LoggingContext
  ): Future[ParameterStorageBackend.LedgerEnd] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.getLedgerEndOffsetAndSequentialId)(
        storageBackend.ledgerEndOrBeforeBegin
      )

  override def lookupInitialLedgerEnd()(implicit
      loggingContext: LoggingContext
  ): Future[Option[Offset]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.getInitialLedgerEnd)(
        storageBackend.ledgerEnd(_).map(_.lastOffset)
      )

  override def initialize(
      ledgerId: LedgerId,
      participantId: ParticipantId,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.initializeLedgerParameters)(
        storageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = ledgerId,
            participantId = participantId,
          )
        )
      )

  override def lookupLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Offset, Configuration)]] =
    dbDispatcher.executeSql(metrics.daml.index.db.lookupConfiguration)(
      storageBackend.ledgerConfiguration(ledgerEnd.get()._1)
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
        val optCurrentConfig = storageBackend.ledgerConfiguration(ledgerEnd.get()._1)(conn)
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
            state.Update.ConfigurationChanged(
              recordTime = Time.Timestamp.assertFromInstant(recordedAt),
              submissionId = Ref.SubmissionId.assertFromString(submissionId),
              participantId =
                Ref.ParticipantId.assertFromString("1"), // not used for DbDto generation
              newConfiguration = configuration,
            )

          case Some(reason) =>
            state.Update.ConfigurationChangeRejected(
              recordTime = Time.Timestamp.assertFromInstant(recordedAt),
              submissionId = Ref.SubmissionId.assertFromString(submissionId),
              participantId =
                Ref.ParticipantId.assertFromString("1"), // not used for DbDto generation
              proposedConfiguration = configuration,
              rejectionReason = reason,
            )
        }

        val offset = validateOffsetStep(offsetStep, conn)
        sequentialIndexer.store(conn, offset, Some(update))
        PersistenceResponse.Ok
      }
    }

  private val NonLocalParticipantId =
    Ref.ParticipantId.assertFromString("RESTRICTED_NON_LOCAL_PARTICIPANT_ID")

  override def storePartyEntry(
      offsetStep: OffsetStep,
      partyEntry: PartyLedgerEntry,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing party entry")
    dbDispatcher.executeSql(metrics.daml.index.db.storePartyEntryDbMetrics) { implicit conn =>
      val offset = validateOffsetStep(offsetStep, conn)
      partyEntry match {
        case PartyLedgerEntry.AllocationAccepted(submissionIdOpt, recordTime, partyDetails) =>
          sequentialIndexer.store(
            conn,
            offset,
            Some(
              state.Update.PartyAddedToParticipant(
                party = partyDetails.party,
                displayName = partyDetails.displayName.orNull,
                // HACK: the `PartyAddedToParticipant` transmits `participantId`s, while here we only have the information
                // whether the party is locally hosted or not. We use the `nonLocalParticipantId` to get the desired effect of
                // the `isLocal = False` information to be transmitted via a `PartyAddedToParticpant` `Update`.
                //
                // This will be properly resolved once we move away from the `sandbox-classic` codebase.
                participantId = if (partyDetails.isLocal) participantId else NonLocalParticipantId,
                recordTime = Time.Timestamp.assertFromInstant(recordTime),
                submissionId = submissionIdOpt,
              )
            ),
          )
          PersistenceResponse.Ok

        case PartyLedgerEntry.AllocationRejected(submissionId, recordTime, reason) =>
          sequentialIndexer.store(
            conn,
            offset,
            Some(
              state.Update.PartyAllocationRejected(
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
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[Ref.WorkflowId],
      transactionId: Ref.TransactionId,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[state.DivulgedContract],
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
      completionInfo: Option[state.CompletionInfo],
      transactionId: Ref.TransactionId,
      recordTime: Instant,
      offsetStep: OffsetStep,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    throw new UnsupportedOperationException(
      "not supported by append-only code"
    ) // TODO append-only: cleanup

  override def storeTransaction(
      preparedInsert: PreparedInsert,
      completionInfo: Option[state.CompletionInfo],
      transactionId: Ref.TransactionId,
      recordTime: Instant,
      ledgerEffectiveTime: Instant,
      offsetStep: OffsetStep,
      transaction: CommittedTransaction,
      divulged: Iterable[state.DivulgedContract],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    throw new UnsupportedOperationException(
      "not supported by append-only code"
    ) // TODO append-only: cleanup

  private def validate(
      ledgerEffectiveTime: Instant,
      transaction: CommittedTransaction,
      divulged: Iterable[state.DivulgedContract],
  )(implicit connection: Connection): Option[PostCommitValidation.Rejection] =
    Timed.value(
      metrics.daml.index.db.storeTransactionDbMetrics.commitValidation,
      postCommitValidation.validate(
        transaction = transaction,
        transactionLedgerEffectiveTime = ledgerEffectiveTime,
        divulged = divulged.iterator.map(_.contractId).toSet,
      ),
    )

  override def storeRejection(
      completionInfo: Option[state.CompletionInfo],
      recordTime: Instant,
      offsetStep: OffsetStep,
      reason: state.Update.CommandRejected.RejectionReasonTemplate,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.storeRejectionDbMetrics) { implicit conn =>
        val offset = validateOffsetStep(offsetStep, conn)
        sequentialIndexer.store(
          conn,
          offset,
          completionInfo.map(info =>
            state.Update.CommandRejected(
              recordTime = Time.Timestamp.assertFromInstant(recordTime),
              completionInfo = info,
              reasonTemplate = reason,
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
        ledgerEntries.foreach { case (offset, entry) =>
          entry match {
            case tx: LedgerEntry.Transaction =>
              val completionInfo = for {
                actAs <- if (tx.actAs.isEmpty) None else Some(tx.actAs)
                applicationId <- tx.applicationId
                commandId <- tx.commandId
                submissionId <- tx.submissionId
              } yield state.CompletionInfo(
                actAs,
                applicationId,
                commandId,
                None,
                Some(submissionId),
              )

              sequentialIndexer.store(
                connection,
                offset,
                Some(
                  state.Update.TransactionAccepted(
                    optCompletionInfo = completionInfo,
                    transactionMeta = state.TransactionMeta(
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
            case LedgerEntry.Rejection(
                  recordTime,
                  commandId,
                  applicationId,
                  submissionId,
                  actAs,
                  reason,
                ) =>
              sequentialIndexer.store(
                connection,
                offset,
                Some(
                  state.Update.CommandRejected(
                    recordTime = Time.Timestamp.assertFromInstant(recordTime),
                    completionInfo = state
                      .CompletionInfo(actAs, applicationId, commandId, None, Some(submissionId)),
                    reasonTemplate = reason.toParticipantStateRejectionReason,
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
      parties: Seq[Ref.Party]
  )(implicit loggingContext: LoggingContext): Future[List[PartyDetails]] =
    if (parties.isEmpty)
      Future.successful(List.empty)
    else
      dbDispatcher
        .executeSql(metrics.daml.index.db.loadParties)(
          storageBackend.parties(parties, ledgerEnd.get()._1)
        )

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadAllParties)(
        storageBackend.knownParties(ledgerEnd.get()._1)
      )

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[Ref.PackageId, PackageDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadPackages)(storageBackend.lfPackages(ledgerEnd.get()._1))

  override def getLfArchive(
      packageId: Ref.PackageId
  )(implicit loggingContext: LoggingContext): Future[Option[Archive]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadArchive)(
        storageBackend.lfArchive(packageId, ledgerEnd.get()._1)
      )
      .map(_.map(data => ArchiveParser.assertFromByteArray(data)))(
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
            state.Update.PublicPackageUpload(
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
            state.Update.PublicPackageUpload(
              archives = packages.view.map(_._1).toList,
              sourceDescription = packages.headOption.flatMap(
                _._2.sourceDescription
              ),
              recordTime = Time.Timestamp.assertFromInstant(recordTime),
              submissionId = Some(submissionId),
            )

          case Some(PackageLedgerEntry.PackageUploadRejected(submissionId, recordTime, reason)) =>
            state.Update.PublicPackageUploadRejected(
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
      submitters: List[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    val key = DeduplicationKeyMaker.make(commandId, submitters)
    dbDispatcher.executeSql(metrics.daml.index.db.stopDeduplicatingCommandDbMetrics)(
      storageBackend.stopDeduplicatingCommand(key)
    )
  }

  /** Prunes the events and command completions tables.
    *
    * @param pruneUpToInclusive         Offset up to which to prune archived history inclusively.
    * @param pruneAllDivulgedContracts  Enables pruning of all immediately and retroactively divulged contracts
    *                                   up to `pruneUpToInclusive`.
    *
    * NOTE:  Pruning of all divulgence events needs to take into account the following considerations:
    *        1.  Migration from mutating schema to append-only schema:
    *            - Divulgence events ingested prior to the migration to the append-only schema do not have offsets assigned
    *            and cannot be pruned incrementally (i.e. respecting the `pruneUpToInclusive)`.
    *            - For this reason, when `pruneAllDivulgedContracts` is set, `pruneUpToInclusive` must be after
    *            the last ingested event offset before the migration, otherwise an INVALID_ARGUMENT response is returned.
    *            - On the first call with `pruneUpToInclusive` higher than the migration offset, all divulgence events are pruned.
    *
    *        2.  Backwards compatibility restriction with regard to transaction-local divulgence in the WriteService:
    *            - Ledgers populated with WriteService versions that do not forward transaction-local divulgence
    *            will hydrate the index with the divulgence events only once for a specific contract-party divulgence relationship
    *            regardless of the number of re-divulgences of the contract to the same party have occurred after the initial one.
    *            - In this case, pruning of all divulged contracts might lead to interpretation failures for command submissions despite
    *            them relying on divulgences that happened after the `pruneUpToInclusive` offset.
    *            - We thus recommend participant node operators in the SDK Docs to either not prune all divulgance events; or wait
    *            for a sufficient amount of time until the Daml application had time to redivulge all events using
    *            transaction-local divulgence.
    *
    *        3.  Backwards compatibility restriction with regard to backfilling lookups:
    *            - Ledgers populated with an old KV WriteService that does not forward divulged contract instances
    *            to the ReadService (see [[com.daml.ledger.participant.state.kvutils.committer.transaction.TransactionCommitter.blind]])
    *            will hydrate the divulgence entries in the index without the create argument and template id.
    *            - During command interpretation, on looking up a divulged contract, the create argument and template id
    *            are backfilled from previous creates/immediate divulgence entries.
    *            - In the case of pruning of all divulged contracts (which includes immediate divulgence pruning),
    *            the previously-mentioned backfilling lookup might fail and lead to interpretation failures
    *            for command submissions that rely on divulged contracts whose associated immediate divulgence event has been pruned.
    *            As for Consideration 2, we thus recommend participant node operators in the SDK Docs to either not prune all divulgance events; or wait
    *            for a sufficient amount of time until the Daml application had time to redivulge all events using
    *            transaction-local divulgence.
    */
  override def prune(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    val allDivulgencePruningParticle =
      if (pruneAllDivulgedContracts) " (including all divulged contracts)" else ""
    logger.info(
      s"Pruning the ledger api server index db$allDivulgencePruningParticle up to ${pruneUpToInclusive.toHexString}."
    )

    dbDispatcher
      .executeSql(metrics.daml.index.db.pruneDbMetrics) { conn =>
        storageBackend.validatePruningOffsetAgainstMigration(
          pruneUpToInclusive,
          pruneAllDivulgedContracts,
          conn,
        )

        storageBackend.pruneEvents(pruneUpToInclusive, pruneAllDivulgedContracts)(
          conn,
          loggingContext,
        )

        storageBackend.pruneCompletions(pruneUpToInclusive)(conn, loggingContext)
        storageBackend.updatePrunedUptoInclusive(pruneUpToInclusive)(conn)

        if (pruneAllDivulgedContracts) {
          storageBackend.updatePrunedAllDivulgedContractsUpToInclusive(pruneUpToInclusive)(conn)
        }
      }
      .andThen {
        case Success(_) =>
          logger.info(
            s"Completed pruning of the ledger api server index db$allDivulgencePruningParticle"
          )
        case Failure(ex) =>
          logger.warn("Pruning failed", ex)
      }(servicesExecutionContext)
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
      ledgerEnd = ledgerEnd,
      stringInterningCache = stringInterningCache,
    )(
      servicesExecutionContext
    )

  override val contractsReader: ContractsReader =
    ContractsReader(dbDispatcher, metrics, storageBackend, ledgerEnd, stringInterningCache)(
      servicesExecutionContext
    )

  override val completions: CommandCompletionsReader =
    new CommandCompletionsReader(
      dbDispatcher,
      storageBackend,
      queryNonPruned,
      metrics,
      stringInterningCache,
    )

  private val postCommitValidation =
    if (performPostCommitValidation)
      new PostCommitValidation.BackedBy(storageBackend, validatePartyAllocation, ledgerEnd)
    else
      PostCommitValidation.Skip

  /** This is a combined store transaction method to support sandbox-classic and tests
    * !!! Usage of this is discouraged, with the removal of sandbox-classic this will be removed
    */
  override def storeTransaction(
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[Ref.WorkflowId],
      transactionId: Ref.TransactionId,
      ledgerEffectiveTime: Instant,
      offsetStep: OffsetStep,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[state.DivulgedContract],
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
                state.Update.TransactionAccepted(
                  optCompletionInfo = completionInfo,
                  transactionMeta = state.TransactionMeta(
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

            case Some(reason) =>
              completionInfo.map(info =>
                state.Update.CommandRejected(
                  recordTime = Time.Timestamp.assertFromInstant(recordTime),
                  completionInfo = info,
                  reasonTemplate = reason.toStateV2RejectionReason,
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
        val actualEnd = storageBackend.ledgerEndOrBeforeBegin(conn).lastOffset
        if (actualEnd.compareTo(p) != 0) throw LedgerEndUpdateError(p) else o
      case CurrentOffset(o) => o
    }
  }

  override def updateStringInterningCache(lastStringInterningId: Int)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = synchronized { // TODO because we first dereference, then update
    import scala.util.chaining._
    val currentCache = stringInterningCache.get()
    if (lastStringInterningId == currentCache.lastId)
      Future.unit
    else
      dbDispatcher.executeSql(metrics.daml.index.db.storeTransactionDbMetrics)(
        conn => // TODO FIXME db metrics
          storageBackend
            .loadStringInterningEntries(currentCache.lastId, lastStringInterningId)(conn)
            .pipe(StringInterningCache.from(_, currentCache))
            .pipe(stringInterningCache.set)
            .tap(_ =>
              println(
                s"update on read side to $lastStringInterningId so size will be now ${stringInterningCache.get().map.size}"
              )
            )
      )
  }

  override def updateLedgerEnd(offset: Offset, eventSeqId: Long): Unit =
    ledgerEnd.set(offset -> eventSeqId)
}

private[platform] object JdbcLedgerDao {

  object Logging {
    def submissionId(id: String): LoggingEntry =
      "submissionId" -> id

    def transactionId(id: Ref.TransactionId): LoggingEntry =
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
      participantId: Ref.ParticipantId,
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
      enricher: Option[ValueEnricher],
      participantId: Ref.ParticipantId,
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
      participantId: Ref.ParticipantId,
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
      participantId: Ref.ParticipantId,
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
      enricher: Option[ValueEnricher],
      participantId: Ref.ParticipantId,
      compressionStrategy: CompressionStrategy,
  )(implicit loggingContext: LoggingContext): ResourceOwner[LedgerDao] = {
    val dbType = DbType.jdbcType(jdbcUrl)
    val storageBackend = StorageBackend.of(dbType)
    for {
      dbDispatcher <- DbDispatcher.owner(
        storageBackend.createDataSource(jdbcUrl),
        serverRole,
        connectionPoolSize,
        connectionTimeout,
        metrics,
      )
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
      new AtomicReference(StringInterningCache.from(Nil)),
      new AtomicReference(Offset.beforeBegin -> 0L), // TODO fix constants
    )
  }

  val acceptType = "accept"
  val rejectType = "reject"
}
