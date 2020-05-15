// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao

import java.sql.Connection
import java.time.Instant
import java.util.concurrent.Executors
import java.util.{Date, UUID}

import akka.NotUsed
import akka.stream.scaladsl.Source
import anorm.SqlParser._
import anorm.ToStatement.optionToStatement
import anorm.{BatchSql, Macro, NamedParameter, ResultSetParser, RowParser, SQL, SqlParser}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{LedgerId, PartyDetails}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationDuplicate,
  CommandDeduplicationNew,
  CommandDeduplicationResult,
  PackageDetails
}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.{EventId, WorkflowId}
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.transaction.Node
import com.daml.lf.value.Value.{AbsoluteContractId, NodeId}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.configuration.ServerRole
import com.daml.platform.events.EventIdFormatter.split
import com.daml.platform.store.Conversions._
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store._
import com.daml.platform.store.dao.JdbcLedgerDao.{H2DatabaseQueries, PostgresQueries}
import com.daml.platform.store.dao.events.{
  ContractsReader,
  PostCommitValidation,
  TransactionsReader,
  TransactionsWriter
}
import com.daml.platform.store.dao.CommandCompletionsTable.{
  prepareCompletionInsert,
  prepareRejectionInsert
}
import com.daml.platform.store.dao.PersistenceResponse.Ok
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}
import com.daml.resources.ResourceOwner
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

private final case class ParsedPartyData(
    party: String,
    displayName: Option[String],
    ledgerOffset: Offset,
    explicit: Boolean)

private final case class ParsedPackageData(
    packageId: String,
    sourceDescription: Option[String],
    size: Long,
    knownSince: Date)

private final case class ParsedCommandData(deduplicateUntil: Instant)

private class JdbcLedgerDao(
    override val maxConcurrentConnections: Int,
    dbDispatcher: DbDispatcher,
    dbType: DbType,
    executionContext: ExecutionContext,
    eventsPageSize: Int,
    performPostCommitValidation: Boolean,
    metrics: Metrics,
)(implicit logCtx: LoggingContext)
    extends LedgerDao {

  private val queries = dbType match {
    case DbType.Postgres => PostgresQueries
    case DbType.H2Database => H2DatabaseQueries
  }

  private val logger = ContextualizedLogger.get(this.getClass)

  private val SQL_SELECT_LEDGER_ID = SQL("select ledger_id from parameters")

  override def currentHealth(): HealthStatus = dbDispatcher.currentHealth()

  override def lookupLedgerId(): Future[Option[LedgerId]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.getLedgerId) { implicit conn =>
        SQL_SELECT_LEDGER_ID
          .as(ledgerString("ledger_id").map(id => LedgerId(id.toString)).singleOpt)
      }

  private val SQL_SELECT_LEDGER_END = SQL("select ledger_end from parameters")

  override def lookupLedgerEnd(): Future[Offset] =
    dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEnd) { implicit conn =>
      SQL_SELECT_LEDGER_END
        .as(offset("ledger_end").single)
    }

  private val SQL_SELECT_INITIAL_LEDGER_END = SQL("select ledger_end from parameters")

  override def lookupInitialLedgerEnd(): Future[Option[Offset]] =
    dbDispatcher.executeSql(metrics.daml.index.db.getInitialLedgerEnd) { implicit conn =>
      SQL_SELECT_INITIAL_LEDGER_END
        .as(offset("ledger_end").?.single)
    }

  private val SQL_INITIALIZE = SQL(
    "insert into parameters(ledger_id, ledger_end) VALUES({LedgerId}, {LedgerEnd})")

  override def initializeLedger(ledgerId: LedgerId, ledgerEnd: Offset): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.initializeLedgerParameters) { implicit conn =>
      val _ = SQL_INITIALIZE
        .on("LedgerId" -> ledgerId.unwrap, "LedgerEnd" -> ledgerEnd)
        .execute()
      ()
    }

  // Note that the ledger entries grow monotonically, however we store many ledger entries in parallel,
  // and thus we need to make sure to only update the ledger end when the ledger entry we're committing
  // is advancing it.
  private val SQL_UPDATE_LEDGER_END = SQL(
    "update parameters set ledger_end = {LedgerEnd} where ledger_end < {LedgerEnd}")

  private def updateLedgerEnd(ledgerEnd: Offset)(implicit conn: Connection): Unit = {
    SQL_UPDATE_LEDGER_END
      .on("LedgerEnd" -> ledgerEnd)
      .execute()
    ()
  }

  private val SQL_UPDATE_CURRENT_CONFIGURATION = SQL(
    "update parameters set configuration={configuration}"
  )
  private val SQL_SELECT_CURRENT_CONFIGURATION = SQL(
    "select ledger_end, configuration from parameters")

  private val SQL_GET_CONFIGURATION_ENTRIES = SQL(
    "select * from configuration_entries where ledger_offset > {startExclusive} and ledger_offset <= {endInclusive} order by ledger_offset asc limit {pageSize} offset {queryOffset}")

  private def updateCurrentConfiguration(configBytes: Array[Byte])(
      implicit conn: Connection): Unit = {
    SQL_UPDATE_CURRENT_CONFIGURATION
      .on("configuration" -> configBytes)
      .execute()
    ()
  }

  private val currentConfigurationParser: ResultSetParser[Option[(Offset, Configuration)]] =
    (offset("ledger_end") ~
      byteArray("configuration").? map flatten).single
      .map {
        case (_, None) => None
        case (offset, Some(configBytes)) =>
          Configuration
            .decode(configBytes)
            .toOption
            .map(config => offset -> config)
      }

  private def selectLedgerConfiguration(implicit conn: Connection) =
    SQL_SELECT_CURRENT_CONFIGURATION.as(currentConfigurationParser)

  override def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]] =
    dbDispatcher.executeSql(metrics.daml.index.db.lookupConfiguration)(implicit conn =>
      selectLedgerConfiguration)

  private val acceptType = "accept"
  private val rejectType = "reject"

  private val configurationEntryParser: RowParser[(Offset, ConfigurationEntry)] =
    (offset("ledger_offset") ~
      str("typ") ~
      str("submission_id") ~
      str("participant_id") ~
      str("rejection_reason").map(s => if (s.isEmpty) null else s).? ~
      byteArray("configuration"))
      .map(flatten)
      .map {
        case (offset, typ, submissionId, participantIdRaw, rejectionReason, configBytes) =>
          val config = Configuration
            .decode(configBytes)
            .fold(err => sys.error(s"Failed to decode configuration: $err"), identity)
          val participantId = ParticipantId
            .fromString(participantIdRaw)
            .fold(
              err => sys.error(s"Failed to decode participant id in configuration entry: $err"),
              identity)

          offset ->
            (typ match {
              case `acceptType` =>
                ConfigurationEntry.Accepted(
                  submissionId = submissionId,
                  participantId = participantId,
                  configuration = config
                )
              case `rejectType` =>
                ConfigurationEntry.Rejected(
                  submissionId = submissionId,
                  participantId = participantId,
                  rejectionReason = rejectionReason.getOrElse("<missing reason>"),
                  proposedConfiguration = config
                )

              case _ =>
                sys.error(s"getConfigurationEntries: Unknown configuration entry type: $typ")
            })
      }

  override def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, ConfigurationEntry), NotUsed] =
    PaginatingAsyncStream(PageSize) { queryOffset =>
      dbDispatcher.executeSql(
        metrics.daml.index.db.loadConfigurationEntries,
        Some(
          s"bounds: ]${startExclusive.toApiString}, ${endInclusive.toApiString}] queryOffset $queryOffset")) {
        implicit conn =>
          SQL_GET_CONFIGURATION_ENTRIES
            .on(
              "startExclusive" -> startExclusive,
              "endInclusive" -> endInclusive,
              "pageSize" -> PageSize,
              "queryOffset" -> queryOffset)
            .asVectorOf(configurationEntryParser)
      }
    }

  private val SQL_INSERT_CONFIGURATION_ENTRY =
    SQL(
      """insert into configuration_entries(ledger_offset, recorded_at, submission_id, participant_id, typ, rejection_reason, configuration)
        |values({ledger_offset}, {recorded_at}, {submission_id}, {participant_id}, {typ}, {rejection_reason}, {configuration})
        |""".stripMargin)

  override def storeConfigurationEntry(
      offset: Offset,
      recordedAt: Instant,
      submissionId: String,
      participantId: ParticipantId,
      configuration: Configuration,
      rejectionReason: Option[String]
  ): Future[PersistenceResponse] = {
    dbDispatcher.executeSql(
      metrics.daml.index.db.storeConfigurationEntryDao,
      Some(s"submissionId=$submissionId"),
    ) { implicit conn =>
      val optCurrentConfig = selectLedgerConfiguration
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
              s"Generation mismatch: expected=$expGeneration, actual=${configuration.generation}")

          case _ =>
            // Rejection reason was set, or we have no previous configuration generation, in which case we accept any
            // generation.
            rejectionReason
        }

      updateLedgerEnd(offset)
      val configurationBytes = Configuration.encode(configuration).toByteArray
      val typ = if (finalRejectionReason.isEmpty) {
        acceptType
      } else {
        rejectType
      }

      Try({
        SQL_INSERT_CONFIGURATION_ENTRY
          .on(
            "ledger_offset" -> offset,
            "recorded_at" -> recordedAt,
            "submission_id" -> submissionId,
            "participant_id" -> participantId,
            "typ" -> typ,
            "rejection_reason" -> finalRejectionReason.orNull,
            "configuration" -> configurationBytes
          )
          .execute()

        if (typ == acceptType) {
          updateCurrentConfiguration(configurationBytes)
        }

        PersistenceResponse.Ok
      }).recover {
        case NonFatal(e) if e.getMessage.contains(queries.DUPLICATE_KEY_ERROR) =>
          logger.warn(
            s"Ignoring duplicate configuration submission, submissionId=$submissionId, participantId=$participantId")
          conn.rollback()
          PersistenceResponse.Duplicate
      }.get

    }
  }

  private val SQL_INSERT_PARTY_ENTRY_ACCEPT =
    SQL(
      """insert into party_entries(ledger_offset, recorded_at, submission_id, participant_id, typ, party, display_name, is_local)
        |values ({ledger_offset}, {recorded_at}, {submission_id}, {participant_id}, 'accept', {party}, {display_name}, {is_local})
        |""".stripMargin)

  private val SQL_INSERT_PARTY_ENTRY_REJECT =
    SQL(
      """insert into party_entries(ledger_offset, recorded_at, submission_id, participant_id, typ, rejection_reason)
        |values ({ledger_offset}, {recorded_at}, {submission_id}, {participant_id}, 'reject', {rejection_reason})
        |""".stripMargin)

  override def storePartyEntry(
      offset: Offset,
      partyEntry: PartyLedgerEntry,
  ): Future[PersistenceResponse] = {
    dbDispatcher.executeSql(metrics.daml.index.db.storePartyEntryDao) { implicit conn =>
      updateLedgerEnd(offset)

      partyEntry match {
        case PartyLedgerEntry.AllocationAccepted(
            submissionIdOpt,
            participantId,
            recordTime,
            partyDetails) =>
          Try({
            SQL_INSERT_PARTY_ENTRY_ACCEPT
              .on(
                "ledger_offset" -> offset,
                "recorded_at" -> recordTime,
                "submission_id" -> submissionIdOpt,
                "participant_id" -> participantId,
                "party" -> partyDetails.party,
                "display_name" -> partyDetails.displayName,
                "is_local" -> partyDetails.isLocal,
              )
              .execute()
            SQL_INSERT_PARTY
              .on(
                "party" -> partyDetails.party,
                "display_name" -> partyDetails.displayName,
                "ledger_offset" -> offset,
              )
              .execute()
            PersistenceResponse.Ok
          }).recover {
            case NonFatal(e) if e.getMessage.contains(queries.DUPLICATE_KEY_ERROR) =>
              logger.warn(
                s"Ignoring duplicate party submission with ID ${partyDetails.party} for submissionId $submissionIdOpt, participantId $participantId")
              conn.rollback()
              PersistenceResponse.Duplicate
          }.get
        case PartyLedgerEntry.AllocationRejected(submissionId, participantId, recordTime, reason) =>
          SQL_INSERT_PARTY_ENTRY_REJECT
            .on(
              "ledger_offset" -> offset,
              "recorded_at" -> recordTime,
              "submission_id" -> submissionId,
              "participant_id" -> participantId,
              "rejection_reason" -> reason
            )
            .execute()
          PersistenceResponse.Ok
      }
    }

  }

  private val SQL_GET_PARTY_ENTRIES = SQL(
    "select * from party_entries where ledger_offset>{startExclusive} and ledger_offset<={endInclusive} order by ledger_offset asc limit {pageSize} offset {queryOffset}")

  private val partyEntryParser: RowParser[(Offset, PartyLedgerEntry)] =
    (offset("ledger_offset") ~
      date("recorded_at") ~
      ledgerString("submission_id").? ~
      participantId("participant_id").? ~
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
            Some(participantId),
            Some(party),
            displayNameOpt,
            `acceptType`,
            None,
            Some(isLocal)) =>
          offset ->
            PartyLedgerEntry.AllocationAccepted(
              submissionIdOpt,
              participantId,
              recordTime.toInstant,
              PartyDetails(party, displayNameOpt, isLocal))
        case (
            offset,
            recordTime,
            Some(submissionId),
            Some(participantId),
            None,
            None,
            `rejectType`,
            Some(reason),
            None) =>
          offset -> PartyLedgerEntry.AllocationRejected(
            submissionId,
            participantId,
            recordTime.toInstant,
            reason)
        case invalidRow =>
          sys.error(s"getPartyEntries: invalid party entry row: $invalidRow")
      }

  override def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, PartyLedgerEntry), NotUsed] = {
    PaginatingAsyncStream(PageSize) { queryOffset =>
      dbDispatcher.executeSql(
        metrics.daml.index.db.loadPartyEntries,
        Some(
          s"bounds: ]${startExclusive.toApiString}, ${endInclusive.toApiString}] queryOffset $queryOffset")) {
        implicit conn =>
          SQL_GET_PARTY_ENTRIES
            .on(
              "startExclusive" -> startExclusive,
              "endInclusive" -> endInclusive,
              "pageSize" -> PageSize,
              "queryOffset" -> queryOffset)
            .asVectorOf(partyEntryParser)
      }
    }
  }

  override def lookupKey(key: Node.GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]] =
    contractsReader.lookupContractKey(forParty, key)

  private def splitOrThrow(id: EventId): NodeId =
    split(id).fold(sys.error(s"Illegal format for event identifier $id"))(_.nodeId)

  override def storeTransaction(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      recordTime: Instant,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: CommittedTransaction,
      divulged: Iterable[DivulgedContract],
  ): Future[PersistenceResponse] = {
    val preparedTransactionInsert =
      transactionsWriter.prepare(
        submitterInfo = submitterInfo,
        workflowId = workflowId,
        transactionId = transactionId,
        ledgerEffectiveTime = ledgerEffectiveTime,
        offset = offset,
        transaction = transaction,
        divulgedContracts = divulged,
      )
    dbDispatcher
      .executeSql(metrics.daml.index.db.storeTransactionDao) { implicit conn =>
        val error =
          postCommitValidation.validate(
            transaction = transaction,
            transactionLedgerEffectiveTime = ledgerEffectiveTime,
            divulged = divulged.iterator.map(_.contractId).toSet,
          )
        if (error.isEmpty) {
          preparedTransactionInsert.write()
          submitterInfo
            .map(prepareCompletionInsert(_, offset, transactionId, recordTime))
            .foreach(_.execute())
        } else {
          for (info @ SubmitterInfo(submitter, _, commandId, _) <- submitterInfo) {
            stopDeduplicatingCommandSync(domain.CommandId(commandId), submitter)
            prepareRejectionInsert(info, offset, recordTime, error.get).execute()
          }
        }
        updateLedgerEnd(offset)
        Ok
      }
  }

  override def storeRejection(
      submitterInfo: Option[SubmitterInfo],
      recordTime: Instant,
      offset: Offset,
      reason: RejectionReason,
  ): Future[PersistenceResponse] =
    dbDispatcher.executeSql(metrics.daml.index.db.storeRejectionDao) { implicit conn =>
      for (info @ SubmitterInfo(submitter, _, commandId, _) <- submitterInfo) {
        stopDeduplicatingCommandSync(domain.CommandId(commandId), submitter)
        prepareRejectionInsert(info, offset, recordTime, reason).execute()
      }
      updateLedgerEnd(offset)
      Ok
    }

  override def storeInitialState(
      ledgerEntries: Vector[(Offset, LedgerEntry)],
      newLedgerEnd: Offset,
  ): Future[Unit] = {
    dbDispatcher
      .executeSql(
        metrics.daml.index.db.storeInitialStateFromScenario,
        Some(s"saving ${ledgerEntries.size} ledger entries")) { implicit conn =>
        ledgerEntries.foreach {
          case (offset, entry) =>
            entry match {
              case tx: LedgerEntry.Transaction =>
                val submitterInfo =
                  for (submitter <- tx.submittingParty; appId <- tx.applicationId;
                    cmdId <- tx.commandId)
                    yield SubmitterInfo(submitter, appId, cmdId, Instant.EPOCH)
                transactionsWriter
                  .prepare(
                    submitterInfo = submitterInfo,
                    workflowId = tx.workflowId,
                    transactionId = tx.transactionId,
                    ledgerEffectiveTime = tx.ledgerEffectiveTime,
                    offset = offset,
                    transaction = tx.transaction.mapNodeId(splitOrThrow),
                    divulgedContracts = Nil,
                  )
                  .write()
                submitterInfo
                  .map(prepareCompletionInsert(_, offset, tx.transactionId, tx.recordedAt))
                  .foreach(_.execute())
              case LedgerEntry.Rejection(recordTime, commandId, applicationId, submitter, reason) =>
                val _ = prepareRejectionInsert(
                  submitterInfo = SubmitterInfo(submitter, applicationId, commandId, Instant.EPOCH),
                  offset = offset,
                  recordTime = recordTime,
                  reason = toParticipantRejection(reason),
                ).execute()
            }
        }
        updateLedgerEnd(newLedgerEnd)
      }
  }

  private def toParticipantRejection(reason: domain.RejectionReason): RejectionReason =
    reason match {
      case r: domain.RejectionReason.Inconsistent =>
        RejectionReason.Inconsistent(r.description)
      case r: domain.RejectionReason.Disputed =>
        RejectionReason.Disputed(r.description)
      case r: domain.RejectionReason.OutOfQuota =>
        RejectionReason.ResourcesExhausted(r.description)
      case r: domain.RejectionReason.PartyNotKnownOnLedger =>
        RejectionReason.PartyNotKnownOnLedger(r.description)
      case r: domain.RejectionReason.SubmitterCannotActViaParticipant =>
        RejectionReason.SubmitterCannotActViaParticipant(r.description)
      case r: domain.RejectionReason.InvalidLedgerTime =>
        RejectionReason.InvalidLedgerTime(r.description)
    }

  private val PageSize = 100

  override def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId],
  ): Future[Option[Instant]] =
    contractsReader.lookupMaximumLedgerTime(contractIds)

  override def lookupActiveOrDivulgedContract(
      contractId: AbsoluteContractId,
      forParty: Party,
  ): Future[Option[AbsoluteContractInst]] =
    contractsReader.lookupActiveContract(forParty, contractId)

  private val SQL_SELECT_MULTIPLE_PARTIES =
    SQL(
      "select party, display_name, ledger_offset, explicit from parties where party in ({parties})")

  private val SQL_SELECT_ALL_PARTIES =
    SQL("select party, display_name, ledger_offset, explicit from parties")

  private val PartyDataParser: RowParser[ParsedPartyData] =
    Macro.parser[ParsedPartyData](
      "party",
      "display_name",
      "ledger_offset",
      "explicit"
    )

  override def getParties(parties: Seq[Party]): Future[List[PartyDetails]] =
    if (parties.isEmpty)
      Future.successful(List.empty)
    else
      dbDispatcher
        .executeSql(metrics.daml.index.db.loadParties) { implicit conn =>
          SQL_SELECT_MULTIPLE_PARTIES
            .on("parties" -> parties)
            .as(PartyDataParser.*)
        }
        .map(_.map(constructPartyDetails))(executionContext)

  override def listKnownParties(): Future[List[PartyDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadAllParties) { implicit conn =>
        SQL_SELECT_ALL_PARTIES
          .as(PartyDataParser.*)
      }
      .map(_.map(constructPartyDetails))(executionContext)

  private def constructPartyDetails(data: ParsedPartyData): PartyDetails =
    // TODO: isLocal should be based on equality of participantId reported in an
    //       update and the id given to participant in a command-line argument
    //       (See issue #2026)
    PartyDetails(Party.assertFromString(data.party), data.displayName, isLocal = true)

  private val SQL_INSERT_PARTY =
    SQL("""insert into parties(party, display_name, ledger_offset, explicit)
        |values ({party}, {display_name}, {ledger_offset}, 'true')""".stripMargin)

  private val SQL_SELECT_PACKAGES =
    SQL("""select package_id, source_description, known_since, size
          |from packages
          |""".stripMargin)

  private val SQL_SELECT_PACKAGE =
    SQL("""select package
          |from packages
          |where package_id = {package_id}
          |""".stripMargin)

  private val PackageDataParser: RowParser[ParsedPackageData] =
    Macro.parser[ParsedPackageData](
      "package_id",
      "source_description",
      "size",
      "known_since"
    )

  override def listLfPackages: Future[Map[PackageId, PackageDetails]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadPackages) { implicit conn =>
        SQL_SELECT_PACKAGES
          .as(PackageDataParser.*)
      }
      .map(
        _.map(
          d =>
            PackageId.assertFromString(d.packageId) -> PackageDetails(
              d.size,
              d.knownSince.toInstant,
              d.sourceDescription)).toMap)(executionContext)

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.loadArchive, Some(s"pkg id: $packageId")) { implicit conn =>
        SQL_SELECT_PACKAGE
          .on(
            "package_id" -> packageId
          )
          .as[Option[Array[Byte]]](SqlParser.byteArray("package").singleOpt)
      }
      .map(_.map(data => Archive.parseFrom(Decode.damlLfCodedInputStreamFromBytes(data))))(
        executionContext)

  private val SQL_INSERT_PACKAGE_ENTRY_ACCEPT =
    SQL("""insert into package_entries(ledger_offset, recorded_at, submission_id, typ)
      |values ({ledger_offset}, {recorded_at}, {submission_id}, 'accept')
      |""".stripMargin)

  private val SQL_INSERT_PACKAGE_ENTRY_REJECT =
    SQL(
      """insert into package_entries(ledger_offset, recorded_at, submission_id, typ, rejection_reason)
      |values ({ledger_offset}, {recorded_at}, {submission_id}, 'reject', {rejection_reason})
      |""".stripMargin)

  override def storePackageEntry(
      offset: Offset,
      packages: List[(Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry]
  ): Future[PersistenceResponse] = {
    dbDispatcher.executeSql(
      metrics.daml.index.db.storePackageEntryDao,
      Some(s"packages: ${packages.map(_._1.getHash).mkString(", ")}")) { implicit conn =>
      updateLedgerEnd(offset)

      if (packages.nonEmpty) {
        val uploadId = optEntry.map(_.submissionId).getOrElse(UUID.randomUUID().toString)
        uploadLfPackages(uploadId, packages)
      }

      optEntry.foreach {
        case PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTime) =>
          SQL_INSERT_PACKAGE_ENTRY_ACCEPT
            .on(
              "ledger_offset" -> offset,
              "recorded_at" -> recordTime,
              "submission_id" -> submissionId,
            )
            .execute()
        case PackageLedgerEntry.PackageUploadRejected(submissionId, recordTime, reason) =>
          SQL_INSERT_PACKAGE_ENTRY_REJECT
            .on(
              "ledger_offset" -> offset,
              "recorded_at" -> recordTime,
              "submission_id" -> submissionId,
              "rejection_reason" -> reason
            )
            .execute()
      }
      PersistenceResponse.Ok
    }
  }

  private def uploadLfPackages(uploadId: String, packages: List[(Archive, PackageDetails)])(
      implicit conn: Connection): Unit = {
    val params = packages
      .map(
        p =>
          Seq[NamedParameter](
            "package_id" -> p._1.getHash,
            "upload_id" -> uploadId,
            "source_description" -> p._2.sourceDescription,
            "size" -> p._2.size,
            "known_since" -> p._2.knownSince,
            "package" -> p._1.toByteArray
        )
      )
    val _ = executeBatchSql(queries.SQL_INSERT_PACKAGE, params)
  }

  private val SQL_GET_PACKAGE_ENTRIES = SQL(
    "select * from package_entries where ledger_offset>{startExclusive} and ledger_offset<={endInclusive} order by ledger_offset asc limit {pageSize} offset {queryOffset}")

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
      endInclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed] = {
    PaginatingAsyncStream(PageSize) { queryOffset =>
      dbDispatcher.executeSql(
        metrics.daml.index.db.loadPackageEntries,
        Some(
          s"bounds: ]${startExclusive.toApiString}, ${endInclusive.toApiString}] queryOffset $queryOffset")) {
        implicit conn =>
          SQL_GET_PACKAGE_ENTRIES
            .on(
              "startExclusive" -> startExclusive,
              "endInclusive" -> endInclusive,
              "pageSize" -> PageSize,
              "queryOffset" -> queryOffset)
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

  private def deduplicationKey(
      commandId: domain.CommandId,
      submitter: Ref.Party,
  ): String = commandId.unwrap + "%" + submitter

  override def deduplicateCommand(
      commandId: domain.CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    dbDispatcher.executeSql(metrics.daml.index.db.deduplicateCommandDao) { implicit conn =>
      val key = deduplicationKey(commandId, submitter)
      // Insert a new deduplication entry, or update an expired entry
      val updated = SQL(queries.SQL_INSERT_COMMAND)
        .on(
          "deduplicationKey" -> key,
          "submittedAt" -> submittedAt,
          "deduplicateUntil" -> deduplicateUntil)
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

  override def removeExpiredDeduplicationData(currentTime: Instant): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.removeExpiredDeduplicationDataDao) {
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

  private[this] def stopDeduplicatingCommandSync(commandId: domain.CommandId, submitter: Party)(
      implicit conn: Connection): Unit = {
    val key = deduplicationKey(commandId, submitter)
    SQL_DELETE_COMMAND
      .on("deduplicationKey" -> key)
      .execute()
    ()
  }

  override def stopDeduplicatingCommand(
      commandId: domain.CommandId,
      submitter: Party): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.stopDeduplicatingCommandDao) { implicit conn =>
      stopDeduplicatingCommandSync(commandId, submitter)
    }

  override def reset(): Future[Unit] =
    dbDispatcher.executeSql(metrics.daml.index.db.truncateAllTables) { implicit conn =>
      val _ = SQL(queries.SQL_TRUNCATE_TABLES).execute()
    }

  private val transactionsWriter: TransactionsWriter =
    new TransactionsWriter(dbType, metrics)

  override val transactionsReader: TransactionsReader =
    new TransactionsReader(dbDispatcher, eventsPageSize, metrics)(executionContext)

  private val contractsReader: ContractsReader =
    ContractsReader(dbDispatcher, dbType, metrics)(executionContext)

  override val completions: CommandCompletionsReader =
    new CommandCompletionsReader(dbDispatcher, metrics)

  private val postCommitValidation =
    if (performPostCommitValidation)
      new PostCommitValidation.BackedBy(contractsReader.committedContracts)
    else
      PostCommitValidation.Skip

  private def executeBatchSql(query: String, params: Iterable[Seq[NamedParameter]])(
      implicit con: Connection) = {
    require(params.nonEmpty, "batch sql statement must have at least one set of name parameters")
    BatchSql(query, params.head, params.drop(1).toArray: _*).execute()
  }
}

object JdbcLedgerDao {

  private val DefaultNumberOfShortLivedConnections = 16

  def readOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      eventsPageSize: Int,
      metrics: Metrics,
  )(implicit logCtx: LoggingContext): ResourceOwner[LedgerReadDao] = {
    val maxConnections = DefaultNumberOfShortLivedConnections
    owner(serverRole, jdbcUrl, maxConnections, eventsPageSize, validate = false, metrics)
      .map(new MeteredLedgerReadDao(_, metrics))
  }

  def writeOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      eventsPageSize: Int,
      metrics: Metrics
  )(implicit logCtx: LoggingContext): ResourceOwner[LedgerDao] = {
    val dbType = DbType.jdbcType(jdbcUrl)
    val maxConnections =
      if (dbType.supportsParallelWrites) DefaultNumberOfShortLivedConnections else 1
    owner(serverRole, jdbcUrl, maxConnections, eventsPageSize, validate = false, metrics)
      .map(new MeteredLedgerDao(_, metrics))
  }

  def validatingWriteOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      eventsPageSize: Int,
      metrics: Metrics,
  )(implicit logCtx: LoggingContext): ResourceOwner[LedgerDao] = {
    val dbType = DbType.jdbcType(jdbcUrl)
    val maxConnections =
      if (dbType.supportsParallelWrites) DefaultNumberOfShortLivedConnections else 1
    owner(serverRole, jdbcUrl, maxConnections, eventsPageSize, validate = true, metrics)
      .map(new MeteredLedgerDao(_, metrics))
  }

  private def owner(
      serverRole: ServerRole,
      jdbcUrl: String,
      maxConnections: Int,
      eventsPageSize: Int,
      validate: Boolean,
      metrics: Metrics,
  )(implicit logCtx: LoggingContext): ResourceOwner[LedgerDao] =
    for {
      dbDispatcher <- DbDispatcher.owner(serverRole, jdbcUrl, maxConnections, metrics)
      executor <- ResourceOwner.forExecutorService(() => Executors.newWorkStealingPool())
    } yield
      new JdbcLedgerDao(
        maxConnections,
        dbDispatcher,
        DbType.jdbcType(jdbcUrl),
        ExecutionContext.fromExecutor(executor),
        eventsPageSize,
        validate,
        metrics
      )

  sealed trait Queries {

    protected[JdbcLedgerDao] def SQL_INSERT_PACKAGE: String

    protected[JdbcLedgerDao] def SQL_IMPLICITLY_INSERT_PARTIES: String

    protected[JdbcLedgerDao] def SQL_INSERT_COMMAND: String

    protected[JdbcLedgerDao] def SQL_TRUNCATE_TABLES: String

    // TODO: Avoid brittleness of error message checks
    protected[JdbcLedgerDao] def DUPLICATE_KEY_ERROR: String
  }

  object PostgresQueries extends Queries {

    override protected[JdbcLedgerDao] val SQL_INSERT_PACKAGE: String =
      """insert into packages(package_id, upload_id, source_description, size, known_since, ledger_offset, package)
        |select {package_id}, {upload_id}, {source_description}, {size}, {known_since}, ledger_end, {package}
        |from parameters
        |on conflict (package_id) do nothing""".stripMargin

    override protected[JdbcLedgerDao] val SQL_IMPLICITLY_INSERT_PARTIES: String =
      """insert into parties(party, explicit, ledger_offset)
        |values({name}, {explicit}, {ledger_offset})
        |on conflict (party) do nothing""".stripMargin

    override protected[JdbcLedgerDao] val SQL_INSERT_COMMAND: String =
      """insert into participant_command_submissions as pcs (deduplication_key, deduplicate_until)
        |values ({deduplicationKey}, {deduplicateUntil})
        |on conflict (deduplication_key)
        |  do update
        |  set deduplicate_until={deduplicateUntil}
        |  where pcs.deduplicate_until < {submittedAt}""".stripMargin

    override protected[JdbcLedgerDao] val DUPLICATE_KEY_ERROR: String = "duplicate key"

    override protected[JdbcLedgerDao] val SQL_TRUNCATE_TABLES: String =
      """truncate table configuration_entries cascade;
        |truncate table package_entries cascade;
        |truncate table parameters cascade;
        |truncate table participant_command_completions cascade;
        |truncate table participant_command_submissions cascade;
        |truncate table participant_events cascade;
        |truncate table participant_event_flat_transaction_witnesses cascade;
        |truncate table participant_event_transaction_tree_witnesses cascade;
        |truncate table participant_contracts cascade;
        |truncate table participant_contract_witnesses cascade;
        |truncate table parties cascade;
        |truncate table party_entries cascade;
      """.stripMargin

  }

  object H2DatabaseQueries extends Queries {

    override protected[JdbcLedgerDao] val SQL_INSERT_PACKAGE: String =
      """merge into packages using dual on package_id = {package_id}
        |when not matched then insert (package_id, upload_id, source_description, size, known_since, ledger_offset, package)
        |select {package_id}, {upload_id}, {source_description}, {size}, {known_since}, ledger_end, {package}
        |from parameters""".stripMargin

    override protected[JdbcLedgerDao] val SQL_IMPLICITLY_INSERT_PARTIES: String =
      """merge into parties using dual on party = {name}
        |when not matched then insert (party, explicit, ledger_offset) values ({name}, {explicit}, {ledger_offset})""".stripMargin

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
        |truncate table participant_event_flat_transaction_witnesses;
        |truncate table participant_event_transaction_tree_witnesses;
        |truncate table participant_contracts;
        |truncate table participant_contract_witnesses;
        |truncate table parties;
        |truncate table party_entries;
        |set referential_integrity true;
      """.stripMargin

  }
}
