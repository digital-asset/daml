// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao

import java.sql.Connection
import java.time.Instant
import java.util.{Date, UUID}

import akka.NotUsed
import akka.stream.scaladsl.Source
import anorm.SqlParser._
import anorm.ToStatement.optionToStatement
import anorm.{BatchSql, Macro, NamedParameter, RowParser, SQL, SqlParser}
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
import com.daml.lf.transaction.{BlindingInfo, GlobalKey}
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.{CurrentOffset, OffsetStep}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store._
import com.daml.platform.store.dao.CommandCompletionsTable.{
  prepareCompletionInsert,
  prepareCompletionsDelete,
  prepareRejectionInsert,
}
import com.daml.platform.store.dao.PersistenceResponse.Ok
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert
import com.daml.platform.store.dao.events._
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
    lfValueTranslationCache: LfValueTranslation.Cache,
    validatePartyAllocation: Boolean,
    idempotentEntryInserts: Boolean,
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

  private val acceptType = "accept"
  private val rejectType = "reject"

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

  private val SQL_INSERT_CONFIGURATION_ENTRY =
    SQL(
      """insert into configuration_entries(ledger_offset, recorded_at, submission_id, typ, rejection_reason, configuration)
        |values({ledger_offset}, {recorded_at}, {submission_id}, {typ}, {rejection_reason}, {configuration})
        |""".stripMargin
    )

  override def storeConfigurationEntry(
      offsetStep: OffsetStep,
      recordedAt: Instant,
      submissionId: String,
      configuration: Configuration,
      rejectionReason: Option[String],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    withEnrichedLoggingContext(Logging.submissionId(submissionId)) { implicit loggingContext =>
      logger.info("Storing a configuration entry")
      dbDispatcher.executeSql(
        metrics.daml.index.db.storeConfigurationEntryDbMetrics
      ) { implicit conn =>
        val optCurrentConfig = ParametersTable.getLedgerEndAndConfiguration(conn)
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

        ParametersTable.updateLedgerEnd(offsetStep)
        val savepoint = conn.setSavepoint()
        val configurationBytes = Configuration.encode(configuration).toByteArray
        val typ = if (finalRejectionReason.isEmpty) {
          acceptType
        } else {
          rejectType
        }

        Try({
          SQL_INSERT_CONFIGURATION_ENTRY
            .on(
              "ledger_offset" -> offsetStep.offset,
              "recorded_at" -> recordedAt,
              "submission_id" -> submissionId,
              "typ" -> typ,
              "rejection_reason" -> finalRejectionReason.orNull,
              "configuration" -> configurationBytes,
            )
            .execute()

          if (typ == acceptType) {
            ParametersTable.updateConfiguration(configurationBytes)
          }

          PersistenceResponse.Ok
        }).recover {
          case NonFatal(e) if e.getMessage.contains(queries.DUPLICATE_KEY_ERROR) =>
            logger.warn(s"Ignoring duplicate configuration submission, submissionId=$submissionId")
            conn.rollback(savepoint)
            PersistenceResponse.Duplicate
        }.get
      }
    }

  private val SQL_INSERT_PARTY_ENTRY_ACCEPT =
    SQL(
      """insert into party_entries(ledger_offset, recorded_at, submission_id, typ, party, display_name, is_local)
        |values ({ledger_offset}, {recorded_at}, {submission_id}, 'accept', {party}, {display_name}, {is_local})
        |""".stripMargin
    )

  private val SQL_INSERT_PARTY_ENTRY_REJECT =
    SQL(
      """insert into party_entries(ledger_offset, recorded_at, submission_id, typ, rejection_reason)
        |values ({ledger_offset}, {recorded_at}, {submission_id}, 'reject', {rejection_reason})
        |""".stripMargin
    )

  override def storePartyEntry(
      offsetStep: OffsetStep,
      partyEntry: PartyLedgerEntry,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing party entry")
    dbDispatcher.executeSql(metrics.daml.index.db.storePartyEntryDbMetrics) { implicit conn =>
      ParametersTable.updateLedgerEnd(offsetStep)
      val savepoint = conn.setSavepoint()

      partyEntry match {
        case PartyLedgerEntry.AllocationAccepted(submissionIdOpt, recordTime, partyDetails) =>
          Try({
            SQL_INSERT_PARTY_ENTRY_ACCEPT
              .on(
                "ledger_offset" -> offsetStep.offset,
                "recorded_at" -> recordTime,
                "submission_id" -> submissionIdOpt,
                "party" -> partyDetails.party,
                "display_name" -> partyDetails.displayName,
                "is_local" -> partyDetails.isLocal,
              )
              .execute()
            SQL_INSERT_PARTY
              .on(
                "party" -> partyDetails.party,
                "display_name" -> partyDetails.displayName,
                "ledger_offset" -> offsetStep.offset,
                "is_local" -> partyDetails.isLocal,
              )
              .execute()
            PersistenceResponse.Ok
          }).recover {
            case NonFatal(e) if e.getMessage.contains(queries.DUPLICATE_KEY_ERROR) =>
              logger.warn(
                s"Ignoring duplicate party submission with ID ${partyDetails.party} for submissionId $submissionIdOpt"
              )
              conn.rollback(savepoint)
              PersistenceResponse.Duplicate
          }.get
        case PartyLedgerEntry.AllocationRejected(submissionId, recordTime, reason) =>
          SQL_INSERT_PARTY_ENTRY_REJECT
            .on(
              "ledger_offset" -> offsetStep.offset,
              "recorded_at" -> recordTime,
              "submission_id" -> submissionId,
              "rejection_reason" -> reason,
            )
            .execute()
          PersistenceResponse.Ok
      }
    }

  }

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

  override def lookupKey(key: GlobalKey, forParties: Set[Party])(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    contractsReader.lookupContractKey(forParties, key)

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
    transactionsWriter.prepare(
      submitterInfo,
      workflowId,
      transactionId,
      ledgerEffectiveTime,
      offset,
      transaction,
      divulgedContracts,
      blindingInfo,
    )

  private def handleError(
      offset: Offset,
      info: SubmitterInfo,
      recordTime: Instant,
      rejectionReason: RejectionReason,
  )(implicit connection: Connection): Unit = {
    stopDeduplicatingCommandSync(domain.CommandId(info.commandId), info.actAs)
    prepareRejectionInsert(info, offset, recordTime, rejectionReason).execute()
    ()
  }

  override def storeTransactionState(
      preparedInsert: PreparedInsert
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.storeTransactionDbMetrics)(
        preparedInsert.writeState(metrics)(_)
      )
      .map(_ => Ok)(servicesExecutionContext)

  override def storeTransactionEvents(
      preparedInsert: PreparedInsert
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.storeTransactionDbMetrics)(
        preparedInsert.writeEvents(metrics)(_)
      )
      .map(_ => Ok)(servicesExecutionContext)

  override def completeTransaction(
      submitterInfo: Option[SubmitterInfo],
      transactionId: TransactionId,
      recordTime: Instant,
      offsetStep: OffsetStep,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    dbDispatcher
      .executeSql(metrics.daml.index.db.storeTransactionDbMetrics) { implicit conn =>
        insertCompletions(submitterInfo, transactionId, recordTime, offsetStep)
        updateLedgerEnd(offsetStep)
        Ok
      }

  override def storeTransaction(
      preparedInsert: PreparedInsert,
      submitterInfo: Option[SubmitterInfo],
      transactionId: TransactionId,
      recordTime: Instant,
      ledgerEffectiveTime: Instant,
      offsetStep: OffsetStep,
      transaction: CommittedTransaction,
      divulged: Iterable[DivulgedContract],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing transaction")
    dbDispatcher
      .executeSql(metrics.daml.index.db.storeTransactionDbMetrics) { implicit conn =>
        validate(ledgerEffectiveTime, transaction, divulged) match {
          case None =>
            preparedInsert.writeState(metrics)
            preparedInsert.writeEvents(metrics)
            insertCompletions(submitterInfo, transactionId, recordTime, offsetStep)
          case Some(error) =>
            submitterInfo.foreach(handleError(offsetStep.offset, _, recordTime, error))
        }

        updateLedgerEnd(offsetStep)
        Ok
      }
  }

  private def validate(
      ledgerEffectiveTime: Instant,
      transaction: CommittedTransaction,
      divulged: Iterable[DivulgedContract],
  )(implicit connection: Connection) =
    Timed.value(
      metrics.daml.index.db.storeTransactionDbMetrics.commitValidation,
      postCommitValidation.validate(
        transaction = transaction,
        transactionLedgerEffectiveTime = ledgerEffectiveTime,
        divulged = divulged.iterator.map(_.contractId).toSet,
      ),
    )

  private def insertCompletions(
      submitterInfo: Option[SubmitterInfo],
      transactionId: TransactionId,
      recordTime: Instant,
      offsetStep: OffsetStep,
  )(implicit connection: Connection): Unit =
    Timed.value(
      metrics.daml.index.db.storeTransactionDbMetrics.insertCompletion,
      submitterInfo
        .map(prepareCompletionInsert(_, offsetStep.offset, transactionId, recordTime))
        .foreach(_.execute()),
    )

  private def updateLedgerEnd(offsetStep: OffsetStep)(implicit connection: Connection): Unit =
    Timed.value(
      metrics.daml.index.db.storeTransactionDbMetrics.updateLedgerEnd,
      ParametersTable.updateLedgerEnd(offsetStep),
    )

  override def storeRejection(
      submitterInfo: Option[SubmitterInfo],
      recordTime: Instant,
      offsetStep: OffsetStep,
      reason: RejectionReason,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing rejection")
    dbDispatcher.executeSql(metrics.daml.index.db.storeRejectionDbMetrics) { implicit conn =>
      for (info <- submitterInfo) {
        handleError(offsetStep.offset, info, recordTime, reason)
      }
      ParametersTable.updateLedgerEnd(offsetStep)
      Ok
    }
  }

  override def storeInitialState(
      ledgerEntries: Vector[(Offset, LedgerEntry)],
      newLedgerEnd: Offset,
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    logger.info("Storing initial state")
    dbDispatcher.executeSql(metrics.daml.index.db.storeInitialStateFromScenario) {
      implicit connection =>
        queries.enforceSynchronousCommit
        ledgerEntries.foreach { case (offset, entry) =>
          entry match {
            case tx: LedgerEntry.Transaction =>
              val submitterInfo =
                for (
                  appId <- tx.applicationId;
                  actAs <- if (tx.actAs.isEmpty) None else Some(tx.actAs);
                  cmdId <- tx.commandId
                )
                  yield SubmitterInfo(actAs, appId, cmdId, Instant.EPOCH)
              prepareTransactionInsert(
                submitterInfo = submitterInfo,
                workflowId = tx.workflowId,
                transactionId = tx.transactionId,
                ledgerEffectiveTime = tx.ledgerEffectiveTime,
                offset = offset,
                transaction = tx.transaction,
                divulgedContracts = Nil,
                blindingInfo = None,
              ).write(metrics)
              submitterInfo
                .map(prepareCompletionInsert(_, offset, tx.transactionId, tx.recordedAt))
                .foreach(_.execute())
            case LedgerEntry.Rejection(recordTime, commandId, applicationId, actAs, reason) =>
              val _ = prepareRejectionInsert(
                submitterInfo = SubmitterInfo(actAs, applicationId, commandId, Instant.EPOCH),
                offset = offset,
                recordTime = recordTime,
                reason = reason,
              ).execute()
          }
        }
        ParametersTable.updateLedgerEnd(CurrentOffset(newLedgerEnd))
    }
  }

  private val PageSize = 100

  override def lookupMaximumLedgerTime(
      contractIds: Set[ContractId]
  )(implicit loggingContext: LoggingContext): Future[Option[Instant]] =
    contractsReader.lookupMaximumLedgerTime(contractIds)

  override def lookupActiveOrDivulgedContract(
      contractId: ContractId,
      forParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[ContractInst]] =
    contractsReader.lookupActiveContract(forParties, contractId)

  private val SQL_SELECT_ALL_PARTIES =
    SQL("select party, display_name, ledger_offset, explicit, is_local from parties")

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

  private val SQL_INSERT_PARTY =
    SQL("""insert into parties(party, display_name, ledger_offset, explicit, is_local)
        |values ({party}, {display_name}, {ledger_offset}, 'true', {is_local})""".stripMargin)

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

  private val SQL_INSERT_PACKAGE_ENTRY_ACCEPT =
    SQL("""insert into package_entries(ledger_offset, recorded_at, submission_id, typ)
        |values ({ledger_offset}, {recorded_at}, {submission_id}, 'accept')
        |""".stripMargin)

  private val SQL_INSERT_PACKAGE_ENTRY_REJECT =
    SQL(
      """insert into package_entries(ledger_offset, recorded_at, submission_id, typ, rejection_reason)
        |values ({ledger_offset}, {recorded_at}, {submission_id}, 'reject', {rejection_reason})
        |""".stripMargin
    )

  override def storePackageEntry(
      offsetStep: OffsetStep,
      packages: List[(Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] = {
    logger.info("Storing package entry")
    dbDispatcher.executeSql(metrics.daml.index.db.storePackageEntryDbMetrics) {
      implicit connection =>
        ParametersTable.updateLedgerEnd(offsetStep)

        if (packages.nonEmpty) {
          val uploadId =
            optEntry.map(_.submissionId).getOrElse(UUID.randomUUID().toString)
          uploadLfPackages(uploadId, packages)
        }

        optEntry.foreach {
          case PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTime) =>
            SQL_INSERT_PACKAGE_ENTRY_ACCEPT
              .on(
                "ledger_offset" -> offsetStep.offset,
                "recorded_at" -> recordTime,
                "submission_id" -> submissionId,
              )
              .execute()
          case PackageLedgerEntry.PackageUploadRejected(submissionId, recordTime, reason) =>
            SQL_INSERT_PACKAGE_ENTRY_REJECT
              .on(
                "ledger_offset" -> offsetStep.offset,
                "recorded_at" -> recordTime,
                "submission_id" -> submissionId,
                "rejection_reason" -> reason,
              )
              .execute()
        }
        PersistenceResponse.Ok
    }
  }

  private def uploadLfPackages(uploadId: String, packages: List[(Archive, PackageDetails)])(implicit
      conn: Connection
  ): Unit = {
    val params = packages
      .map(p =>
        Seq[NamedParameter](
          "package_id" -> p._1.getHash,
          "upload_id" -> uploadId,
          "source_description" -> p._2.sourceDescription,
          "size" -> p._2.size,
          "known_since" -> p._2.knownSince,
          "package" -> p._1.toByteArray,
        )
      )
    val _ = executeBatchSql(queries.SQL_INSERT_PACKAGE, params)
  }

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

  private def deduplicationKey(
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
      transactionsWriter.prepareEventsDelete(pruneUpToInclusive).execute()
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

  private val compressionStrategy: CompressionStrategy =
    CompressionStrategy.AllGZIP

  private val compressionMetrics: CompressionMetrics =
    CompressionMetrics(metrics)

  private val transactionsWriter: TransactionsWriter =
    new TransactionsWriter(
      dbType,
      metrics,
      translation,
      compressionStrategy,
      compressionMetrics,
      idempotentEntryInserts,
    )

  override val transactionsReader: TransactionsReader =
    new TransactionsReader(dbDispatcher, dbType, eventsPageSize, metrics, translation)(
      servicesExecutionContext
    )

  private val contractsReader: ContractsReader =
    ContractsReader(dbDispatcher, dbType, metrics, lfValueTranslationCache)(
      servicesExecutionContext
    )

  override val completions: CommandCompletionsReader =
    new CommandCompletionsReader(dbDispatcher, dbType, metrics, servicesExecutionContext)

  private val postCommitValidation =
    if (performPostCommitValidation)
      new PostCommitValidation.BackedBy(contractsReader.committedContracts, validatePartyAllocation)
    else
      PostCommitValidation.Skip

  private def executeBatchSql(query: String, params: Iterable[Seq[NamedParameter]])(implicit
      con: Connection
  ) = {
    require(params.nonEmpty, "batch sql statement must have at least one set of name parameters")
    BatchSql(query, params.head, params.drop(1).toSeq: _*).execute()
  }
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
      lfValueTranslationCache: LfValueTranslation.Cache,
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
      idempotentEventInserts = false,
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
      lfValueTranslationCache: LfValueTranslation.Cache,
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
      idempotentEventInserts = dbType == DbType.Postgres,
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
      lfValueTranslationCache: LfValueTranslation.Cache,
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
      idempotentEventInserts = false,
      enricher = enricher,
    ).map(new MeteredLedgerDao(_, metrics))
  }

  private[dao] def selectParties(
      parties: Seq[Party]
  )(implicit connection: Connection): List[ParsedPartyData] =
    SQL_SELECT_MULTIPLE_PARTIES
      .on("parties" -> parties)
      .as(PartyDataParser.*)

  private[dao] def constructPartyDetails(data: ParsedPartyData): PartyDetails =
    PartyDetails(Party.assertFromString(data.party), data.displayName, data.isLocal)

  private val SQL_SELECT_MULTIPLE_PARTIES =
    SQL(
      "select party, display_name, ledger_offset, explicit, is_local from parties where party in ({parties})"
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
      lfValueTranslationCache: LfValueTranslation.Cache,
      validatePartyAllocation: Boolean = false,
      jdbcAsyncCommitMode: DbType.AsyncCommitMode = DbType.SynchronousCommit,
      idempotentEventInserts: Boolean,
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
      idempotentEventInserts,
      enricher,
    )

  sealed trait Queries {

    protected[JdbcLedgerDao] def enforceSynchronousCommit(implicit conn: Connection): Unit

    protected[JdbcLedgerDao] def SQL_INSERT_PACKAGE: String

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
        |truncate table participant_events cascade;
        |truncate table participant_contracts cascade;
        |truncate table participant_contract_witnesses cascade;
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

  object H2DatabaseQueries extends Queries {
    override protected[JdbcLedgerDao] val SQL_INSERT_PACKAGE: String =
      """merge into packages using dual on package_id = {package_id}
        |when not matched then insert (package_id, upload_id, source_description, size, known_since, ledger_offset, package)
        |select {package_id}, {upload_id}, {source_description}, {size}, {known_since}, ledger_end, {package}
        |from parameters""".stripMargin

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
}
