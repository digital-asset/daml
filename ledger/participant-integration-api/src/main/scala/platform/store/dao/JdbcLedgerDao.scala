// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.api.health.{HealthStatus, ReportsHealth}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.daml.ledger.participant.state.index.v2.{IndexerPartyDetails, PackageDetails}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.archive.ArchiveParser
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.entries.LoggingEntry
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.{ApplicationId, PackageId, Party, SubmissionId, TransactionId, WorkflowId}
import com.daml.platform.store._
import com.daml.platform.store.dao.events._
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend.{ParameterStorageBackend, ReadStorageBackend}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.interning.StringInterning
import com.daml.platform.store.utils.QueueBasedConcurrencyLimiter

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private class JdbcLedgerDao(
    dbDispatcher: DbDispatcher with ReportsHealth,
    servicesExecutionContext: ExecutionContext,
    eventsPageSize: Int,
    eventsProcessingParallelism: Int,
    acsIdPageSize: Int,
    acsIdPageBufferSize: Int,
    acsIdPageWorkingMemoryBytes: Int,
    acsIdFetchingParallelism: Int,
    acsContractFetchingParallelism: Int,
    acsGlobalParallelism: Int,
    metrics: Metrics,
    engine: Option[Engine],
    sequentialIndexer: SequentialWriteDao,
    participantId: Ref.ParticipantId,
    readStorageBackend: ReadStorageBackend,
    parameterStorageBackend: ParameterStorageBackend,
    completionsMaxPayloadsPerPayloadsPage: Int,
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
        parameterStorageBackend.ledgerEnd
      )

  case class InvalidLedgerEnd(msg: String) extends RuntimeException(msg)

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
        val update = state.Update.ConfigurationChanged(
          recordTime = recordedAt,
          submissionId = SubmissionId.assertFromString(submissionId),
          participantId = Ref.ParticipantId.assertFromString("1"), // not used for DbDto generation
          newConfiguration = configuration,
        )

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

  private val PageSize = 100

  override def getParties(
      parties: Seq[Party]
  )(implicit loggingContext: LoggingContext): Future[List[IndexerPartyDetails]] =
    if (parties.isEmpty)
      Future.successful(List.empty)
    else
      dbDispatcher
        .executeSql(metrics.daml.index.db.loadParties)(
          readStorageBackend.partyStorageBackend.parties(parties)
        )

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[IndexerPartyDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadAllParties)(
        readStorageBackend.partyStorageBackend.knownParties
      )

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[PackageId, PackageDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadPackages)(
        readStorageBackend.packageStorageBackend.lfPackages
      )

  override def getLfArchive(
      packageId: PackageId
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
          throw LedgerApiErrors.RequestValidation.OffsetOutOfRange
            .Reject(
              "Pruning offset for all divulged contracts needs to be after the migration offset"
            )(new DamlContextualizedErrorLogger(logger, loggingContext, None))
            .asGrpcError
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

  private val translation: LfValueTranslation =
    new LfValueTranslation(
      metrics = metrics,
      engineO = engine,
      loadPackage = (packageId, loggingContext) => this.getLfArchive(packageId)(loggingContext),
    )

  private val queryNonPruned = QueryNonPrunedImpl(parameterStorageBackend)

  override val transactionsReader: TransactionsReader =
    new TransactionsReader(
      dispatcher = dbDispatcher,
      queryNonPruned = queryNonPruned,
      eventStorageBackend = readStorageBackend.eventStorageBackend,
      pageSize = eventsPageSize,
      eventProcessingParallelism = eventsProcessingParallelism,
      metrics = metrics,
      lfValueTranslation = translation,
      acsReader = new FilterTableACSReader(
        dispatcher = dbDispatcher,
        queryNonPruned = queryNonPruned,
        eventStorageBackend = readStorageBackend.eventStorageBackend,
        pageSize = eventsPageSize,
        idPageSize = acsIdPageSize,
        idPageBufferSize = acsIdPageBufferSize,
        idPageWorkingMemoryBytes = acsIdPageWorkingMemoryBytes,
        idFetchingParallelism = acsIdFetchingParallelism,
        acsFetchingparallelism = acsContractFetchingParallelism,
        metrics = metrics,
        querylimiter =
          new QueueBasedConcurrencyLimiter(acsGlobalParallelism, servicesExecutionContext),
        executionContext = servicesExecutionContext,
      ),
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
      pageSize = completionsMaxPayloadsPerPayloadsPage,
    )

  /** This is a combined store transaction method to support sandbox-classic and tests
    * !!! Usage of this is discouraged, with the removal of sandbox-classic this will be removed
    */
  override def storeTransaction(
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
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
              contractMetadata = Map.empty,
            )
          ),
        )
        PersistenceResponse.Ok
      }
  }

  /** Returns all TransactionMetering records matching given criteria */
  override def meteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
  )(implicit loggingContext: LoggingContext): Future[ReportData] = {
    dbDispatcher.executeSql(metrics.daml.index.db.lookupConfiguration)(
      readStorageBackend.meteringStorageBackend.reportData(from, to, applicationId)
    )
  }
}

private[platform] object JdbcLedgerDao {

  object Logging {
    def submissionId(id: String): LoggingEntry =
      "submissionId" -> id

    def transactionId(id: TransactionId): LoggingEntry =
      "transactionId" -> id
  }

  def read(
      dbSupport: DbSupport,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      acsIdPageSize: Int,
      acsIdPageBufferSize: Int,
      acsIdPageWorkingMemoryBytes: Int,
      acsIdFetchingParallelism: Int,
      acsContractFetchingParallelism: Int,
      acsGlobalParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      engine: Option[Engine],
      participantId: Ref.ParticipantId,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      completionsMaxPayloadsPerPayloadsPage: Int,
  ): LedgerReadDao =
    new JdbcLedgerDao(
      dbSupport.dbDispatcher,
      servicesExecutionContext,
      eventsPageSize,
      eventsProcessingParallelism,
      acsIdPageSize,
      acsIdPageBufferSize,
      acsIdPageWorkingMemoryBytes,
      acsIdFetchingParallelism,
      acsContractFetchingParallelism,
      acsGlobalParallelism,
      metrics,
      engine,
      SequentialWriteDao.noop,
      participantId,
      dbSupport.storageBackendFactory.readStorageBackend(ledgerEndCache, stringInterning),
      dbSupport.storageBackendFactory.createParameterStorageBackend,
      completionsMaxPayloadsPerPayloadsPage = completionsMaxPayloadsPerPayloadsPage,
    )

  def write(
      dbSupport: DbSupport,
      sequentialWriteDao: SequentialWriteDao,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      acsIdPageSize: Int,
      acsIdPageBufferSize: Int,
      acsIdPageWorkingMemoryBytes: Int,
      acsIdFetchingParallelism: Int,
      acsContractFetchingParallelism: Int,
      acsGlobalParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      engine: Option[Engine],
      participantId: Ref.ParticipantId,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      completionsMaxPayloadsPerPayloadsPage: Int,
  ): LedgerDao =
    new JdbcLedgerDao(
      dbSupport.dbDispatcher,
      servicesExecutionContext,
      eventsPageSize,
      eventsProcessingParallelism,
      acsIdPageSize,
      acsIdPageBufferSize,
      acsIdPageWorkingMemoryBytes,
      acsIdFetchingParallelism,
      acsContractFetchingParallelism,
      acsGlobalParallelism,
      metrics,
      engine,
      sequentialWriteDao,
      participantId,
      dbSupport.storageBackendFactory.readStorageBackend(ledgerEndCache, stringInterning),
      dbSupport.storageBackendFactory.createParameterStorageBackend,
      completionsMaxPayloadsPerPayloadsPage = completionsMaxPayloadsPerPayloadsPage,
    )

  val acceptType = "accept"
  val rejectType = "reject"
}
