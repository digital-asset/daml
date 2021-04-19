// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.appendonlydao

import java.sql.Connection
import java.time.Instant
import java.util.Date

import akka.NotUsed
import akka.stream.scaladsl.Source
import anorm.SqlParser._
import anorm.{Macro, RowParser, SQL, SqlParser}
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
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.engine.ValueEnricher
import com.daml.lf.transaction.BlindingInfo
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.OffsetStep
import com.daml.platform.store.Conversions._
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store._
import com.daml.platform.store.appendonlydao.CommandCompletionsTable.prepareCompletionsDelete
import com.daml.platform.store.appendonlydao.events.{
  ContractsReader,
  EventsTableDelete,
  LfValueTranslation,
  PostCommitValidation,
  TransactionsReader,
}
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert
import com.daml.platform.store.dao.{
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

private final case class ParsedPartyData(
    party: String,
    displayName: Option[String],
    ledgerOffset: Offset,
    explicit: Boolean,
    isLocal: Boolean,
)

private final case class ParsedPackageData(
    packageId: String,
    sourceDescription: Option[String],
    size: Long,
    knownSince: Date,
)

private final case class ParsedCommandData(deduplicateUntil: Instant)

private class JdbcLedgerDao(
    dbDispatcher: DbDispatcher,
    dbType: DbType,
    servicesExecutionContext: ExecutionContext,
    eventsPageSize: Int,
    performPostCommitValidation: Boolean,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslationCache.Cache,
    validatePartyAllocation: Boolean,
    enricher: Option[ValueEnricher],
) extends LedgerDao {

  import JdbcLedgerDao._

  private val queries = dbType match {
    case DbType.Postgres => PostgresQueries
    case DbType.H2Database => H2DatabaseQueries
  }

  private val logger = ContextualizedLogger.get(this.getClass)

  override def currentHealth(): HealthStatus = dbDispatcher.currentHealth()

  override def lookupLedgerId()(implicit loggingContext: LoggingContext): Future[Option[LedgerId]] =
    dbDispatcher.executeSql(metrics.daml.index.db.getLedgerId)(ParametersTable.getLedgerId)

  override def lookupParticipantId()(implicit
      loggingContext: LoggingContext
  ): Future[Option[ParticipantId]] =
    dbDispatcher.executeSql(metrics.daml.index.db.getParticipantId)(
      ParametersTable.getParticipantId
    )

  /** Defaults to Offset.begin if ledger_end is unset
    */
  override def lookupLedgerEnd()(implicit loggingContext: LoggingContext): Future[Offset] =
    dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEnd)(ParametersTable.getLedgerEnd)

  override def lookupLedgerEndOffsetAndSequentialId()(implicit
      loggingContext: LoggingContext
  ): Future[(Offset, Long)] =
    dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEndOffsetAndSequentialId)(
      ParametersTable.getLedgerEndOffsetAndSequentialId
    )

  override def lookupInitialLedgerEnd()(implicit
      loggingContext: LoggingContext
  ): Future[Option[Offset]] =
    dbDispatcher.executeSql(metrics.daml.index.db.getInitialLedgerEnd)(
      ParametersTable.getInitialLedgerEnd
    )

  override def initializeLedger(
      ledgerId: LedgerId
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.initializeLedgerParameters) {
      implicit connection =>
        queries.enforceSynchronousCommit
        ParametersTable.setLedgerId(ledgerId.unwrap)(connection)
    }

  override def initializeParticipantId(
      participantId: ParticipantId
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.initializeParticipantId) { implicit connection =>
      queries.enforceSynchronousCommit
      ParametersTable.setParticipantId(participantId.unwrap)(connection)
    }

  private val SQL_GET_CONFIGURATION_ENTRIES = SQL(
    "select * from configuration_entries where ledger_offset > {startExclusive} and ledger_offset <= {endInclusive} order by ledger_offset asc limit {pageSize} offset {queryOffset}"
  )

  override def lookupLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Offset, Configuration)]] =
    dbDispatcher.executeSql(metrics.daml.index.db.lookupConfiguration)(
      ParametersTable.getLedgerEndAndConfiguration
    )

  private val configurationEntryParser: RowParser[(Offset, ConfigurationEntry)] =
    (offset("ledger_offset") ~
      str("typ") ~
      str("submission_id") ~
      str("rejection_reason").map(s => if (s.isEmpty) null else s).? ~
      byteArray("configuration"))
      .map(flatten)
      .map { case (offset, typ, submissionId, rejectionReason, configBytes) =>
        val config = Configuration
          .decode(configBytes)
          .fold(err => sys.error(s"Failed to decode configuration: $err"), identity)
        offset ->
          (typ match {
            case `acceptType` =>
              ConfigurationEntry.Accepted(
                submissionId = submissionId,
                configuration = config,
              )
            case `rejectType` =>
              ConfigurationEntry.Rejected(
                submissionId = submissionId,
                rejectionReason = rejectionReason.getOrElse("<missing reason>"),
                proposedConfiguration = config,
              )

            case _ =>
              sys.error(s"getConfigurationEntries: Unknown configuration entry type: $typ")
          })
      }

  override def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, ConfigurationEntry), NotUsed] =
    PaginatingAsyncStream(PageSize) { queryOffset =>
      withEnrichedLoggingContext("queryOffset" -> queryOffset.toString) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.index.db.loadConfigurationEntries) {
          implicit connection =>
            SQL_GET_CONFIGURATION_ENTRIES
              .on(
                "startExclusive" -> startExclusive,
                "endInclusive" -> endInclusive,
                "pageSize" -> PageSize,
                "queryOffset" -> queryOffset,
              )
              .asVectorOf(configurationEntryParser)
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
    throw new UnsupportedOperationException("not supported") // TODO add support

  override def storePartyEntry(
      offsetStep: OffsetStep,
      partyEntry: PartyLedgerEntry,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    throw new UnsupportedOperationException("not supported") // TODO add support

  private val SQL_GET_PARTY_ENTRIES = SQL(
    "select * from party_entries where ledger_offset>{startExclusive} and ledger_offset<={endInclusive} order by ledger_offset asc limit {pageSize} offset {queryOffset}"
  )

  private val partyEntryParser: RowParser[(Offset, PartyLedgerEntry)] =
    (offset("ledger_offset") ~
      date("recorded_at") ~
      ledgerString("submission_id").? ~
      party("party").? ~
      str("display_name").? ~
      str("typ") ~
      str("rejection_reason").? ~
      bool("is_local").?)
      .map(flatten)
      .map {
        case (
              offset,
              recordTime,
              submissionIdOpt,
              Some(party),
              displayNameOpt,
              `acceptType`,
              None,
              Some(isLocal),
            ) =>
          offset ->
            PartyLedgerEntry.AllocationAccepted(
              submissionIdOpt,
              recordTime.toInstant,
              PartyDetails(party, displayNameOpt, isLocal),
            )
        case (
              offset,
              recordTime,
              Some(submissionId),
              None,
              None,
              `rejectType`,
              Some(reason),
              None,
            ) =>
          offset -> PartyLedgerEntry.AllocationRejected(
            submissionId,
            recordTime.toInstant,
            reason,
          )
        case invalidRow =>
          sys.error(s"getPartyEntries: invalid party entry row: $invalidRow")
      }

  override def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, PartyLedgerEntry), NotUsed] = {
    PaginatingAsyncStream(PageSize) { queryOffset =>
      withEnrichedLoggingContext("queryOffset" -> queryOffset.toString) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.index.db.loadPartyEntries) { implicit connection =>
          SQL_GET_PARTY_ENTRIES
            .on(
              "startExclusive" -> startExclusive,
              "endInclusive" -> endInclusive,
              "pageSize" -> PageSize,
              "queryOffset" -> queryOffset,
            )
            .asVectorOf(partyEntryParser)
        }
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

  // TODO this private method is needed for wiring post-commit-validation for sandbox-classic integration later.
  //  Until then it is a protected field, to pass the build. TODO switch back to private.
  protected def validate(
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
    throw new UnsupportedOperationException("not supported") // TODO add support

  override def storeInitialState(
      ledgerEntries: Vector[(Offset, LedgerEntry)],
      newLedgerEnd: Offset,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    throw new UnsupportedOperationException(
      "not supported by append-only code"
    ) // TODO append-only: cleanup

  private val PageSize = 100

  private val SQL_SELECT_ALL_PARTIES =
    SQL(
      "select parties.party, parties.display_name, parties.ledger_offset, parties.explicit, parties.is_local from parties, parameters where parameters.ledger_end >= parties.ledger_offset"
    )

  override def getParties(
      parties: Seq[Party]
  )(implicit loggingContext: LoggingContext): Future[List[PartyDetails]] =
    if (parties.isEmpty)
      Future.successful(List.empty)
    else
      dbDispatcher
        .executeSql(metrics.daml.index.db.loadParties) { implicit conn =>
          selectParties(parties)
        }
        .map(_.map(constructPartyDetails))(servicesExecutionContext)

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadAllParties) { implicit conn =>
        SQL_SELECT_ALL_PARTIES
          .as(PartyDataParser.*)
      }
      .map(_.map(constructPartyDetails))(servicesExecutionContext)

  private val SQL_SELECT_PACKAGES =
    SQL(
      """select packages.package_id, packages.source_description, packages.known_since, packages.size
        |from packages, parameters
        |where packages.ledger_offset <= parameters.ledger_end
        |""".stripMargin
    )

  private val SQL_SELECT_PACKAGE =
    SQL("""select packages.package
        |from packages, parameters
        |where package_id = {package_id}
        |and packages.ledger_offset <= parameters.ledger_end
        |""".stripMargin)

  private val PackageDataParser: RowParser[ParsedPackageData] =
    Macro.parser[ParsedPackageData](
      "package_id",
      "source_description",
      "size",
      "known_since",
    )

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[PackageId, PackageDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadPackages) { implicit conn =>
        SQL_SELECT_PACKAGES
          .as(PackageDataParser.*)
      }
      .map(
        _.map(d =>
          PackageId.assertFromString(d.packageId) -> PackageDetails(
            d.size,
            d.knownSince.toInstant,
            d.sourceDescription,
          )
        ).toMap
      )(servicesExecutionContext)

  override def getLfArchive(
      packageId: PackageId
  )(implicit loggingContext: LoggingContext): Future[Option[Archive]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadArchive) { implicit conn =>
        SQL_SELECT_PACKAGE
          .on(
            "package_id" -> packageId
          )
          .as[Option[Array[Byte]]](SqlParser.byteArray("package").singleOpt)
      }
      .map(_.map(data => Archive.parseFrom(Decode.damlLfCodedInputStreamFromBytes(data))))(
        servicesExecutionContext
      )

  override def storePackageEntry(
      offsetStep: OffsetStep,
      packages: List[(Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    throw new UnsupportedOperationException("not supported") // TODO add support

  private val SQL_GET_PACKAGE_ENTRIES = SQL(
    "select * from package_entries where ledger_offset>{startExclusive} and ledger_offset<={endInclusive} order by ledger_offset asc limit {pageSize} offset {queryOffset}"
  )

  private val packageEntryParser: RowParser[(Offset, PackageLedgerEntry)] =
    (offset("ledger_offset") ~
      date("recorded_at") ~
      ledgerString("submission_id").? ~
      str("typ") ~
      str("rejection_reason").?)
      .map(flatten)
      .map {
        case (offset, recordTime, Some(submissionId), `acceptType`, None) =>
          offset ->
            PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTime.toInstant)
        case (offset, recordTime, Some(submissionId), `rejectType`, Some(reason)) =>
          offset ->
            PackageLedgerEntry.PackageUploadRejected(submissionId, recordTime.toInstant, reason)
        case invalidRow =>
          sys.error(s"packageEntryParser: invalid party entry row: $invalidRow")
      }

  override def getPackageEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, PackageLedgerEntry), NotUsed] =
    PaginatingAsyncStream(PageSize) { queryOffset =>
      withEnrichedLoggingContext("queryOffset" -> queryOffset.toString) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.index.db.loadPackageEntries) { implicit connection =>
          SQL_GET_PACKAGE_ENTRIES
            .on(
              "startExclusive" -> startExclusive,
              "endInclusive" -> endInclusive,
              "pageSize" -> PageSize,
              "queryOffset" -> queryOffset,
            )
            .asVectorOf(packageEntryParser)
        }
      }
    }

  private val SQL_SELECT_COMMAND = SQL("""
      |select deduplicate_until
      |from participant_command_submissions
      |where deduplication_key = {deduplicationKey}
    """.stripMargin)

  private val CommandDataParser: RowParser[ParsedCommandData] =
    Macro.parser[ParsedCommandData](
      "deduplicate_until"
    )

  override def deduplicateCommand(
      commandId: domain.CommandId,
      submitters: List[Ref.Party],
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult] =
    dbDispatcher.executeSql(metrics.daml.index.db.deduplicateCommandDbMetrics) { implicit conn =>
      val key = deduplicationKey(commandId, submitters)
      // Insert a new deduplication entry, or update an expired entry
      val updated = SQL(queries.SQL_INSERT_COMMAND)
        .on(
          "deduplicationKey" -> key,
          "submittedAt" -> submittedAt,
          "deduplicateUntil" -> deduplicateUntil,
        )
        .executeUpdate()

      if (updated == 1) {
        // New row inserted, this is the first time the command is submitted
        CommandDeduplicationNew
      } else {
        // Deduplication row already exists
        val result = SQL_SELECT_COMMAND
          .on("deduplicationKey" -> key)
          .as(CommandDataParser.single)

        CommandDeduplicationDuplicate(result.deduplicateUntil)
      }
    }

  private val SQL_DELETE_EXPIRED_COMMANDS = SQL("""
      |delete from participant_command_submissions
      |where deduplicate_until < {currentTime}
    """.stripMargin)

  override def removeExpiredDeduplicationData(
      currentTime: Instant
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.removeExpiredDeduplicationDataDbMetrics) {
      implicit conn =>
        SQL_DELETE_EXPIRED_COMMANDS
          .on("currentTime" -> currentTime)
          .execute()
        ()
    }

  private val SQL_DELETE_COMMAND = SQL("""
      |delete from participant_command_submissions
      |where deduplication_key = {deduplicationKey}
    """.stripMargin)

  private[this] def stopDeduplicatingCommandSync(
      commandId: domain.CommandId,
      submitters: List[Party],
  )(implicit conn: Connection): Unit = {
    val key = deduplicationKey(commandId, submitters)
    SQL_DELETE_COMMAND
      .on("deduplicationKey" -> key)
      .execute()
    ()
  }

  override def stopDeduplicatingCommand(
      commandId: domain.CommandId,
      submitters: List[Party],
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.stopDeduplicatingCommandDbMetrics) {
      implicit conn =>
        stopDeduplicatingCommandSync(commandId, submitters)
    }

  private val SQL_UPDATE_MOST_RECENT_PRUNING = SQL("""
      |update parameters set participant_pruned_up_to_inclusive={pruned_up_to_inclusive}
      |where participant_pruned_up_to_inclusive < {pruned_up_to_inclusive} or participant_pruned_up_to_inclusive is null
      |""".stripMargin)

  private def updateMostRecentPruning(
      prunedUpToInclusive: Offset
  )(implicit conn: Connection): Unit = {
    SQL_UPDATE_MOST_RECENT_PRUNING
      .on("pruned_up_to_inclusive" -> prunedUpToInclusive)
      .execute()
    ()
  }

  override def prune(
      pruneUpToInclusive: Offset
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.pruneDbMetrics) { implicit conn =>
      EventsTableDelete.prepareEventsDelete(pruneUpToInclusive).execute()
      prepareCompletionsDelete(pruneUpToInclusive).execute()
      updateMostRecentPruning(pruneUpToInclusive)
      logger.info(s"Pruned ledger api server index db up to ${pruneUpToInclusive.toHexString}")
    }

  override def reset()(implicit loggingContext: LoggingContext): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.truncateAllTables) { implicit conn =>
      val _ = SQL(queries.SQL_TRUNCATE_TABLES).execute()
    }

  private val translation: LfValueTranslation =
    new LfValueTranslation(
      cache = lfValueTranslationCache,
      metrics = metrics,
      enricherO = enricher,
      loadPackage = (packageId, loggingContext) => this.getLfArchive(packageId)(loggingContext),
    )

  override val transactionsReader: TransactionsReader =
    new TransactionsReader(dbDispatcher, dbType, eventsPageSize, metrics, translation)(
      servicesExecutionContext
    )

  override val contractsReader: ContractsReader =
    ContractsReader(dbDispatcher, dbType, metrics)(
      servicesExecutionContext
    )

  override val completions: CommandCompletionsReader =
    new CommandCompletionsReader(dbDispatcher, dbType, metrics, servicesExecutionContext)

  private val postCommitValidation =
    if (performPostCommitValidation)
      new PostCommitValidation.BackedBy(contractsReader.committedContracts, validatePartyAllocation)
    else
      PostCommitValidation.Skip
}

private[platform] object JdbcLedgerDao {

  object Logging {

    def submissionId(id: String): (String, String) = "submissionId" -> id

    def transactionId(id: TransactionId): (String, String) =
      "transactionId" -> id

  }

  def readOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      connectionPoolSize: Int,
      eventsPageSize: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      enricher: Option[ValueEnricher],
  )(implicit loggingContext: LoggingContext): ResourceOwner[LedgerReadDao] = {
    owner(
      serverRole,
      jdbcUrl,
      connectionPoolSize,
      eventsPageSize,
      validate = false,
      servicesExecutionContext,
      metrics,
      lfValueTranslationCache,
      enricher = enricher,
    ).map(new MeteredLedgerReadDao(_, metrics))
  }

  def writeOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      connectionPoolSize: Int,
      eventsPageSize: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      jdbcAsyncCommitMode: DbType.AsyncCommitMode,
      enricher: Option[ValueEnricher],
  )(implicit loggingContext: LoggingContext): ResourceOwner[LedgerDao] = {
    val dbType = DbType.jdbcType(jdbcUrl)
    owner(
      serverRole,
      jdbcUrl,
      dbType.maxSupportedWriteConnections(connectionPoolSize),
      eventsPageSize,
      validate = false,
      servicesExecutionContext,
      metrics,
      lfValueTranslationCache,
      jdbcAsyncCommitMode =
        if (dbType.supportsAsynchronousCommits) jdbcAsyncCommitMode else DbType.SynchronousCommit,
      enricher = enricher,
    ).map(new MeteredLedgerDao(_, metrics))
  }

  def validatingWriteOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      connectionPoolSize: Int,
      eventsPageSize: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      validatePartyAllocation: Boolean = false,
      enricher: Option[ValueEnricher],
  )(implicit loggingContext: LoggingContext): ResourceOwner[LedgerDao] = {
    val dbType = DbType.jdbcType(jdbcUrl)
    owner(
      serverRole,
      jdbcUrl,
      dbType.maxSupportedWriteConnections(connectionPoolSize),
      eventsPageSize,
      validate = true,
      servicesExecutionContext,
      metrics,
      lfValueTranslationCache,
      validatePartyAllocation,
      enricher = enricher,
    ).map(new MeteredLedgerDao(_, metrics))
  }

  private[appendonlydao] def selectParties(
      parties: Seq[Party]
  )(implicit connection: Connection): List[ParsedPartyData] =
    SQL_SELECT_MULTIPLE_PARTIES
      .on("parties" -> parties)
      .as(PartyDataParser.*)

  private[appendonlydao] def constructPartyDetails(data: ParsedPartyData): PartyDetails =
    PartyDetails(Party.assertFromString(data.party), data.displayName, data.isLocal)

  private val SQL_SELECT_MULTIPLE_PARTIES =
    SQL(
      "select parties.party, parties.display_name, parties.ledger_offset, parties.explicit, parties.is_local from parties, parameters where party in ({parties}) and parties.ledger_offset <= parameters.ledger_end"
    )

  private val PartyDataParser: RowParser[ParsedPartyData] =
    Macro.parser[ParsedPartyData](
      "party",
      "display_name",
      "ledger_offset",
      "explicit",
      "is_local",
    )

  private def owner(
      serverRole: ServerRole,
      jdbcUrl: String,
      connectionPoolSize: Int,
      eventsPageSize: Int,
      validate: Boolean,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      validatePartyAllocation: Boolean = false,
      jdbcAsyncCommitMode: DbType.AsyncCommitMode = DbType.SynchronousCommit,
      enricher: Option[ValueEnricher],
  )(implicit loggingContext: LoggingContext): ResourceOwner[LedgerDao] =
    for {
      dbDispatcher <- DbDispatcher.owner(
        serverRole,
        jdbcUrl,
        connectionPoolSize,
        250.millis,
        metrics,
        jdbcAsyncCommitMode,
      )
    } yield new JdbcLedgerDao(
      dbDispatcher,
      DbType.jdbcType(jdbcUrl),
      servicesExecutionContext,
      eventsPageSize,
      validate,
      metrics,
      lfValueTranslationCache,
      validatePartyAllocation,
      enricher,
    )

  sealed trait Queries {

    protected[JdbcLedgerDao] def enforceSynchronousCommit(implicit conn: Connection): Unit

    protected[JdbcLedgerDao] def SQL_INSERT_COMMAND: String

    protected[JdbcLedgerDao] def SQL_TRUNCATE_TABLES: String

    // TODO: Avoid brittleness of error message checks
    protected[JdbcLedgerDao] def DUPLICATE_KEY_ERROR: String
  }

  object PostgresQueries extends Queries {
    override protected[JdbcLedgerDao] val SQL_INSERT_COMMAND: String =
      """insert into participant_command_submissions as pcs (deduplication_key, deduplicate_until)
        |values ({deduplicationKey}, {deduplicateUntil})
        |on conflict (deduplication_key)
        |  do update
        |  set deduplicate_until={deduplicateUntil}
        |  where pcs.deduplicate_until < {submittedAt}""".stripMargin

    override protected[JdbcLedgerDao] val DUPLICATE_KEY_ERROR: String =
      "duplicate key"

    override protected[JdbcLedgerDao] val SQL_TRUNCATE_TABLES: String =
      """truncate table configuration_entries cascade;
        |truncate table package_entries cascade;
        |truncate table parameters cascade;
        |truncate table participant_command_completions cascade;
        |truncate table participant_command_submissions cascade;
        |truncate table participant_events_divulgence cascade;
        |truncate table participant_events_create cascade;
        |truncate table participant_events_consuming_exercise cascade;
        |truncate table participant_events_non_consuming_exercise cascade;
        |truncate table parties cascade;
        |truncate table party_entries cascade;
      """.stripMargin

    override protected[JdbcLedgerDao] def enforceSynchronousCommit(implicit
        conn: Connection
    ): Unit = {
      val statement =
        conn.prepareStatement("SET LOCAL synchronous_commit = 'on'")
      try {
        statement.execute()
        ()
      } finally {
        statement.close()
      }
    }
  }

  // TODO H2 support
  object H2DatabaseQueries extends Queries {
    override protected[JdbcLedgerDao] val SQL_INSERT_COMMAND: String =
      """merge into participant_command_submissions pcs
        |using dual on deduplication_key = {deduplicationKey}
        |when not matched then
        |  insert (deduplication_key, deduplicate_until)
        |  values ({deduplicationKey}, {deduplicateUntil})
        |when matched and pcs.deduplicate_until < {submittedAt} then
        |  update set deduplicate_until={deduplicateUntil}""".stripMargin

    override protected[JdbcLedgerDao] val DUPLICATE_KEY_ERROR: String =
      "Unique index or primary key violation"

    override protected[JdbcLedgerDao] val SQL_TRUNCATE_TABLES: String =
      """set referential_integrity false;
        |truncate table configuration_entries;
        |truncate table package_entries;
        |truncate table parameters;
        |truncate table participant_command_completions;
        |truncate table participant_command_submissions;
        |truncate table participant_events;
        |truncate table participant_contracts;
        |truncate table participant_contract_witnesses;
        |truncate table parties;
        |truncate table party_entries;
        |set referential_integrity true;
      """.stripMargin

    /** H2 does not support asynchronous commits */
    override protected[JdbcLedgerDao] def enforceSynchronousCommit(implicit
        conn: Connection
    ): Unit = ()
  }

  def deduplicationKey(
      commandId: domain.CommandId,
      submitters: List[Ref.Party],
  ): String = {
    val submitterPart =
      if (submitters.length == 1)
        submitters.head
      else
        submitters.sorted(Ordering.String).distinct.mkString("%")
    commandId.unwrap + "%" + submitterPart
  }

  val acceptType = "accept"
  val rejectType = "reject"
}
