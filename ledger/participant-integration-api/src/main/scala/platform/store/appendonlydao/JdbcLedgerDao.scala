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
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.lf.archive.Decode
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.engine.ValueEnricher
import com.daml.lf.transaction.BlindingInfo
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.{CurrentOffset, IncrementalOffsetStep, OffsetStep}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store._
import com.daml.platform.store.appendonlydao.CommandCompletionsTable.prepareCompletionsDelete
import com.daml.platform.store.appendonlydao.events.{
  CompressionStrategy,
  ContractsReader,
  EventsTableDelete,
  LfValueTranslation,
  PostCommitValidation,
  TransactionsReader,
}
import com.daml.platform.store.backend.{StorageBackend, UpdateToDBDTOV1}
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
    sequentialIndexer: SequentialWriteDao,
    participantId: v1.ParticipantId,
) extends LedgerDao {

  import JdbcLedgerDao._

  private val queries = dbType match {
    case DbType.Postgres => PostgresQueries
    case DbType.H2Database => H2DatabaseQueries
    case DbType.Oracle => throw new NotImplementedError("not yet supported")
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
    """select
      |    configuration_entries.ledger_offset,
      |    configuration_entries.recorded_at,
      |    configuration_entries.submission_id,
      |    configuration_entries.typ,
      |    configuration_entries.configuration,
      |    configuration_entries.rejection_reason
      |  from
      |    configuration_entries,
      |    parameters
      |  where
      |    ledger_offset > {startExclusive} and
      |    ledger_offset <= {endInclusive} and
      |    parameters.ledger_end >= ledger_offset
      |  order by ledger_offset asc
      |  limit {pageSize}
      |  offset {queryOffset}""".stripMargin
  )

  private val SQL_GET_LATEST_CONFIGURATION_ENTRY = SQL(
    s"""select
      |    configuration_entries.ledger_offset,
      |    configuration_entries.recorded_at,
      |    configuration_entries.submission_id,
      |    configuration_entries.typ,
      |    configuration_entries.configuration,
      |    configuration_entries.rejection_reason
      |  from
      |    configuration_entries,
      |    parameters
      |  where
      |    configuration_entries.typ = '$acceptType' and
      |    parameters.ledger_end >= ledger_offset
      |  order by ledger_offset desc
      |  limit 1""".stripMargin
  )

  private def lookupLedgerConfiguration(connection: Connection): Option[(Offset, Configuration)] =
    SQL_GET_LATEST_CONFIGURATION_ENTRY
      .on()
      .asVectorOf(configurationEntryParser)(connection)
      .collectFirst { case (offset, ConfigurationEntry.Accepted(_, configuration)) =>
        offset -> configuration
      }

  override def lookupLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Offset, Configuration)]] =
    dbDispatcher.executeSql(metrics.daml.index.db.lookupConfiguration)(lookupLedgerConfiguration)

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
    withEnrichedLoggingContext(Logging.submissionId(submissionId)) { implicit loggingContext =>
      logger.info("Storing a configuration entry")
      dbDispatcher.executeSql(
        metrics.daml.index.db.storeConfigurationEntryDbMetrics
      ) { implicit conn =>
        val optCurrentConfig = lookupLedgerConfiguration(conn)
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
                v1.ParticipantId.assertFromString("1"), // not used for DBDTO generation
              newConfiguration = configuration,
            )

          case Some(reason) =>
            Update.ConfigurationChangeRejected(
              recordTime = Time.Timestamp.assertFromInstant(recordedAt),
              submissionId = SubmissionId.assertFromString(submissionId),
              participantId =
                v1.ParticipantId.assertFromString("1"), // not used for DBDTO generation
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
          case NonFatal(e) if e.getMessage.contains(queries.DUPLICATE_KEY_ERROR) =>
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
            case NonFatal(e) if e.getMessage.contains(queries.DUPLICATE_KEY_ERROR) =>
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
        queries.enforceSynchronousCommit
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
                      submissionTime = null, // not used for DBDTO generation
                      submissionSeed = null, // not used for DBDTO generation
                      optUsedPackages = None, // not used for DBDTO generation
                      optNodeSeeds = None, // not used for DBDTO generation
                      optByKeyNodes = None, // not used for DBDTO generation
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
      val key = DeduplicationKeyMaker.make(commandId, submitters)
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
    val key = DeduplicationKeyMaker.make(commandId, submitters)
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
                    submissionTime = null, // not used for DBDTO generation
                    submissionSeed = null, // not used for DBDTO generation
                    optUsedPackages = None, // not used for DBDTO generation
                    optNodeSeeds = None, // not used for DBDTO generation
                    optByKeyNodes = None, // not used for DBDTO generation
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
        val actualEnd = ParametersTable.getLedgerEnd(conn)
        if (actualEnd.compareTo(p) != 0) throw LedgerEndUpdateError(p) else o
      case CurrentOffset(o) => o
    }
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
      connectionTimeout: FiniteDuration,
      eventsPageSize: Int,
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
      dbType: DbType,
      participantId: v1.ParticipantId,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      metrics: Metrics,
      compressionStrategy: CompressionStrategy,
  ): SequentialWriteDao =
    SequentialWriteDaoImpl(
      storageBackend = StorageBackend.of(dbType),
      updateToDbDtos = UpdateToDBDTOV1(
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
      connectionTimeout: FiniteDuration,
      eventsPageSize: Int,
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
    } yield new JdbcLedgerDao(
      dbDispatcher,
      dbType,
      servicesExecutionContext,
      eventsPageSize,
      validate,
      metrics,
      lfValueTranslationCache,
      validatePartyAllocation,
      enricher,
      sequentialWriteDao(
        dbType,
        participantId,
        lfValueTranslationCache,
        metrics,
        compressionStrategy,
      ),
      participantId,
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

  val acceptType = "accept"
  val rejectType = "reject"
}
