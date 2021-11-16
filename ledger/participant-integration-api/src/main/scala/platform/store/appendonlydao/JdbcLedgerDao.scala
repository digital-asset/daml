// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.appendonlydao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.DamlContextualizedErrorLogger
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
import com.daml.lf.archive.ArchiveParser
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.ValueEnricher
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.entries.LoggingEntry
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.Conversions._
import com.daml.platform.store._
import com.daml.platform.store.appendonlydao.events._
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend.{
  DeduplicationStorageBackend,
  ParameterStorageBackend,
  ReadStorageBackend,
  ResetStorageBackend,
  StorageBackendFactory,
}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry,
}
import com.daml.platform.store.interning.StringInterning

import java.sql.Connection
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
    readStorageBackend: ReadStorageBackend,
    parameterStorageBackend: ParameterStorageBackend,
    deduplicationStorageBackend: DeduplicationStorageBackend,
    resetStorageBackend: ResetStorageBackend,
    errorFactories: ErrorFactories,
) extends LedgerDao {

  import JdbcLedgerDao._

  private val logger = ContextualizedLogger.get(this.getClass)

  override def currentHealth(): HealthStatus = dbDispatcher.currentHealth()

  override def lookupLedgerId()(implicit loggingContext: LoggingContext): Future[Option[LedgerId]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.getLedgerId)(
        parameterStorageBackend.ledgerIdentity(_).map(_.ledgerId)
      )

  override def lookupParticipantId()(implicit
      loggingContext: LoggingContext
  ): Future[Option[ParticipantId]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.getParticipantId)(
        parameterStorageBackend.ledgerIdentity(_).map(_.participantId)
      )

  /** Defaults to Offset.begin if ledger_end is unset
    */
  override def lookupLedgerEnd()(implicit loggingContext: LoggingContext): Future[LedgerEnd] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.getLedgerEnd)(
        parameterStorageBackend.ledgerEndOrBeforeBegin
      )

  case class InvalidLedgerEnd(msg: String) extends RuntimeException(msg)

  override def lookupInitialLedgerEnd()(implicit
      loggingContext: LoggingContext
  ): Future[Option[Offset]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.getInitialLedgerEnd)(
        parameterStorageBackend.ledgerEnd(_).map(_.lastOffset)
      )

  override def initialize(
      ledgerId: LedgerId,
      participantId: ParticipantId,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.initializeLedgerParameters)(
        parameterStorageBackend.initializeParameters(
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
      readStorageBackend.configurationStorageBackend.ledgerConfiguration
    )

  override def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, ConfigurationEntry), NotUsed] =
    PaginatingAsyncStream(PageSize) { queryOffset =>
      withEnrichedLoggingContext("queryOffset" -> queryOffset) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.index.db.loadConfigurationEntries) {
          readStorageBackend.configurationStorageBackend.configurationEntries(
            startExclusive = startExclusive,
            endInclusive = endInclusive,
            pageSize = PageSize,
            queryOffset = queryOffset,
          )
        }
      }
    }

  override def storeConfigurationEntry(
      offset: Offset,
      recordedAt: Timestamp,
      submissionId: String,
      configuration: Configuration,
      rejectionReason: Option[String],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    withEnrichedLoggingContext(Logging.submissionId(submissionId)) { implicit loggingContext =>
      logger.info("Storing configuration entry")
      dbDispatcher.executeSql(
        metrics.daml.index.db.storeConfigurationEntryDbMetrics
      ) { implicit conn =>
        val optCurrentConfig =
          readStorageBackend.configurationStorageBackend.ledgerConfiguration(conn)
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
              recordTime = recordedAt,
              submissionId = Ref.SubmissionId.assertFromString(submissionId),
              participantId =
                Ref.ParticipantId.assertFromString("1"), // not used for DbDto generation
              newConfiguration = configuration,
            )

          case Some(reason) =>
            state.Update.ConfigurationChangeRejected(
              recordTime = recordedAt,
              submissionId = Ref.SubmissionId.assertFromString(submissionId),
              participantId =
                Ref.ParticipantId.assertFromString("1"), // not used for DbDto generation
              proposedConfiguration = configuration,
              rejectionReason = reason,
            )
        }

        sequentialIndexer.store(conn, offset, Some(update))
        PersistenceResponse.Ok
      }
    }

  private val NonLocalParticipantId =
    Ref.ParticipantId.assertFromString("RESTRICTED_NON_LOCAL_PARTICIPANT_ID")

  override def storePartyEntry(
      offset: Offset,
      partyEntry: PartyLedgerEntry,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing party entry")
    dbDispatcher.executeSql(metrics.daml.index.db.storePartyEntryDbMetrics) { implicit conn =>
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
                recordTime = recordTime,
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
                recordTime = recordTime,
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
          readStorageBackend.partyStorageBackend.partyEntries(
            startExclusive = startExclusive,
            endInclusive = endInclusive,
            pageSize = PageSize,
            queryOffset = queryOffset,
          )
        )
      }
    }
  }

  private def validate(
      ledgerEffectiveTime: Timestamp,
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
      recordTime: Timestamp,
      offset: Offset,
      reason: state.Update.CommandRejected.RejectionReasonTemplate,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.storeRejectionDbMetrics) { implicit conn =>
        sequentialIndexer.store(
          conn,
          offset,
          completionInfo.map(info =>
            state.Update.CommandRejected(
              recordTime = recordTime,
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
                      ledgerEffectiveTime = tx.ledgerEffectiveTime,
                      workflowId = tx.workflowId,
                      submissionTime = null, // not used for DbDto generation
                      submissionSeed = null, // not used for DbDto generation
                      optUsedPackages = None, // not used for DbDto generation
                      optNodeSeeds = None, // not used for DbDto generation
                      optByKeyNodes = None, // not used for DbDto generation
                    ),
                    transaction = tx.transaction,
                    transactionId = tx.transactionId,
                    recordTime = tx.recordedAt,
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
                    recordTime = recordTime,
                    completionInfo = state
                      .CompletionInfo(actAs, applicationId, commandId, None, submissionId),
                    reasonTemplate = reason.toParticipantStateRejectionReason(errorFactories)(
                      new DamlContextualizedErrorLogger(logger, loggingContext, submissionId)
                    ),
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
          readStorageBackend.partyStorageBackend.parties(parties)
        )

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadAllParties)(
        readStorageBackend.partyStorageBackend.knownParties
      )

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[Ref.PackageId, PackageDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadPackages)(
        readStorageBackend.packageStorageBackend.lfPackages
      )

  override def getLfArchive(
      packageId: Ref.PackageId
  )(implicit loggingContext: LoggingContext): Future[Option[Archive]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadArchive)(
        readStorageBackend.packageStorageBackend.lfArchive(packageId)
      )
      .map(_.map(data => ArchiveParser.assertFromByteArray(data)))(
        servicesExecutionContext
      )

  override def storePackageEntry(
      offset: Offset,
      packages: List[(Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing package entry")
    dbDispatcher.executeSql(metrics.daml.index.db.storePackageEntryDbMetrics) {
      implicit connection =>
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
              recordTime = packages.headOption
                .map(
                  _._2.knownSince
                )
                .getOrElse(Timestamp.Epoch),
              submissionId =
                None, // If the submission ID is missing, this update will not insert a row in the package_entries table
            )

          case Some(PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTime)) =>
            state.Update.PublicPackageUpload(
              archives = packages.view.map(_._1).toList,
              sourceDescription = packages.headOption.flatMap(
                _._2.sourceDescription
              ),
              recordTime = recordTime,
              submissionId = Some(submissionId),
            )

          case Some(PackageLedgerEntry.PackageUploadRejected(submissionId, recordTime, reason)) =>
            state.Update.PublicPackageUploadRejected(
              submissionId = submissionId,
              recordTime = recordTime,
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
          readStorageBackend.packageStorageBackend.packageEntries(
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
      submittedAt: Timestamp,
      deduplicateUntil: Timestamp,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult] =
    dbDispatcher.executeSql(metrics.daml.index.db.deduplicateCommandDbMetrics) { conn =>
      val key = DeduplicationKeyMaker.make(commandId, submitters)
      // Insert a new deduplication entry, or update an expired entry
      val updated = deduplicationStorageBackend.upsertDeduplicationEntry(
        key = key,
        submittedAt = submittedAt,
        deduplicateUntil = deduplicateUntil,
      )(conn)

      if (updated == 1) {
        // New row inserted, this is the first time the command is submitted
        CommandDeduplicationNew
      } else {
        // Deduplication row already exists
        CommandDeduplicationDuplicate(deduplicationStorageBackend.deduplicatedUntil(key)(conn))
      }
    }

  override def removeExpiredDeduplicationData(
      currentTime: Timestamp
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.removeExpiredDeduplicationDataDbMetrics)(
      deduplicationStorageBackend.removeExpiredDeduplicationData(currentTime)
    )

  override def stopDeduplicatingCommand(
      commandId: domain.CommandId,
      submitters: List[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    val key = DeduplicationKeyMaker.make(commandId, submitters)
    dbDispatcher.executeSql(metrics.daml.index.db.stopDeduplicatingCommandDbMetrics)(
      deduplicationStorageBackend.stopDeduplicatingCommand(key)
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
        if (
          !readStorageBackend.eventStorageBackend.isPruningOffsetValidAgainstMigration(
            pruneUpToInclusive,
            pruneAllDivulgedContracts,
            conn,
          )
        ) {
          throw errorFactories.offsetOutOfRange(None)(
            "Pruning offset for all divulged contracts needs to be after the migration offset"
          )(new DamlContextualizedErrorLogger(logger, loggingContext, None))
        }

        readStorageBackend.eventStorageBackend.pruneEvents(
          pruneUpToInclusive,
          pruneAllDivulgedContracts,
        )(
          conn,
          loggingContext,
        )

        readStorageBackend.completionStorageBackend.pruneCompletions(pruneUpToInclusive)(
          conn,
          loggingContext,
        )
        parameterStorageBackend.updatePrunedUptoInclusive(pruneUpToInclusive)(conn)

        if (pruneAllDivulgedContracts) {
          parameterStorageBackend.updatePrunedAllDivulgedContractsUpToInclusive(pruneUpToInclusive)(
            conn
          )
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
    dbDispatcher.executeSql(metrics.daml.index.db.truncateAllTables)(resetStorageBackend.reset)

  private val translation: LfValueTranslation =
    new LfValueTranslation(
      cache = lfValueTranslationCache,
      metrics = metrics,
      enricherO = enricher,
      loadPackage = (packageId, loggingContext) => this.getLfArchive(packageId)(loggingContext),
    )

  private val queryNonPruned = QueryNonPrunedImpl(parameterStorageBackend, errorFactories)

  override val transactionsReader: TransactionsReader =
    new TransactionsReader(
      dispatcher = dbDispatcher,
      queryNonPruned = queryNonPruned,
      eventStorageBackend = readStorageBackend.eventStorageBackend,
      contractStorageBackend = readStorageBackend.contractStorageBackend,
      pageSize = eventsPageSize,
      eventProcessingParallelism = eventsProcessingParallelism,
      metrics = metrics,
      lfValueTranslation = translation,
    )(
      servicesExecutionContext
    )

  override val contractsReader: ContractsReader =
    ContractsReader(dbDispatcher, metrics, readStorageBackend.contractStorageBackend)(
      servicesExecutionContext
    )

  override val completions: CommandCompletionsReader =
    new CommandCompletionsReader(
      dbDispatcher,
      readStorageBackend.completionStorageBackend,
      queryNonPruned,
      metrics,
    )

  private val postCommitValidation =
    if (performPostCommitValidation)
      new PostCommitValidation.BackedBy(
        readStorageBackend.partyStorageBackend,
        readStorageBackend.contractStorageBackend,
        validatePartyAllocation,
      )
    else
      PostCommitValidation.Skip

  /** This is a combined store transaction method to support sandbox-classic and tests
    * !!! Usage of this is discouraged, with the removal of sandbox-classic this will be removed
    */
  override def storeTransaction(
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[Ref.WorkflowId],
      transactionId: Ref.TransactionId,
      ledgerEffectiveTime: Timestamp,
      offset: Offset,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[state.DivulgedContract],
      blindingInfo: Option[BlindingInfo],
      recordTime: Timestamp,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing transaction")
    dbDispatcher
      .executeSql(metrics.daml.index.db.storeTransactionDbMetrics) { implicit conn =>
        sequentialIndexer.store(
          conn,
          offset,
          validate(ledgerEffectiveTime, transaction, divulgedContracts) match {
            case None =>
              Some(
                state.Update.TransactionAccepted(
                  optCompletionInfo = completionInfo,
                  transactionMeta = state.TransactionMeta(
                    ledgerEffectiveTime = ledgerEffectiveTime,
                    workflowId = workflowId,
                    submissionTime = null, // not used for DbDto generation
                    submissionSeed = null, // not used for DbDto generation
                    optUsedPackages = None, // not used for DbDto generation
                    optNodeSeeds = None, // not used for DbDto generation
                    optByKeyNodes = None, // not used for DbDto generation
                  ),
                  transaction = transaction,
                  transactionId = transactionId,
                  recordTime = recordTime,
                  divulgedContracts = divulgedContracts.toList,
                  blindingInfo = blindingInfo,
                )
              )

            case Some(reason) =>
              completionInfo.map(info =>
                state.Update.CommandRejected(
                  recordTime = recordTime,
                  completionInfo = info,
                  reasonTemplate = reason.toStateV2RejectionReason(errorFactories)(
                    new DamlContextualizedErrorLogger(
                      logger,
                      loggingContext,
                      info.submissionId,
                    )
                  ),
                )
              )
          },
        )
        PersistenceResponse.Ok
      }
  }

}

private[platform] object JdbcLedgerDao {

  object Logging {
    def submissionId(id: String): LoggingEntry =
      "submissionId" -> id

    def transactionId(id: Ref.TransactionId): LoggingEntry =
      "transactionId" -> id
  }

  def read(
      dbDispatcher: DbDispatcher,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      enricher: Option[ValueEnricher],
      participantId: Ref.ParticipantId,
      errorFactories: ErrorFactories,
      storageBackendFactory: StorageBackendFactory,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): LedgerReadDao =
    new MeteredLedgerReadDao(
      new JdbcLedgerDao(
        dbDispatcher,
        servicesExecutionContext,
        eventsPageSize,
        eventsProcessingParallelism,
        false,
        metrics,
        lfValueTranslationCache,
        false,
        enricher,
        SequentialWriteDao.noop,
        participantId,
        storageBackendFactory.readStorageBackend(ledgerEndCache, stringInterning),
        storageBackendFactory.createParameterStorageBackend,
        storageBackendFactory.createDeduplicationStorageBackend,
        storageBackendFactory.createResetStorageBackend,
        errorFactories,
      ),
      metrics,
    )

  def write(
      dbDispatcher: DbDispatcher,
      sequentialWriteDao: SequentialWriteDao,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      enricher: Option[ValueEnricher],
      participantId: Ref.ParticipantId,
      errorFactories: ErrorFactories,
      storageBackendFactory: StorageBackendFactory,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): LedgerDao =
    new MeteredLedgerDao(
      new JdbcLedgerDao(
        dbDispatcher,
        servicesExecutionContext,
        eventsPageSize,
        eventsProcessingParallelism,
        false,
        metrics,
        lfValueTranslationCache,
        false,
        enricher,
        sequentialWriteDao,
        participantId,
        storageBackendFactory.readStorageBackend(ledgerEndCache, stringInterning),
        storageBackendFactory.createParameterStorageBackend,
        storageBackendFactory.createDeduplicationStorageBackend,
        storageBackendFactory.createResetStorageBackend,
        errorFactories,
      ),
      metrics,
    )

  def validatingWrite(
      dbDispatcher: DbDispatcher,
      sequentialWriteDao: SequentialWriteDao,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      validatePartyAllocation: Boolean = false,
      enricher: Option[ValueEnricher],
      participantId: Ref.ParticipantId,
      errorFactories: ErrorFactories,
      storageBackendFactory: StorageBackendFactory,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): LedgerDao =
    new MeteredLedgerDao(
      new JdbcLedgerDao(
        dbDispatcher,
        servicesExecutionContext,
        eventsPageSize,
        eventsProcessingParallelism,
        true,
        metrics,
        lfValueTranslationCache,
        validatePartyAllocation,
        enricher,
        sequentialWriteDao,
        participantId,
        storageBackendFactory.readStorageBackend(ledgerEndCache, stringInterning),
        storageBackendFactory.createParameterStorageBackend,
        storageBackendFactory.createDeduplicationStorageBackend,
        storageBackendFactory.createResetStorageBackend,
        errorFactories,
      ),
      metrics,
    )

  val acceptType = "accept"
  val rejectType = "reject"
}
