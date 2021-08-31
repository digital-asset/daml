// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import java.time.Instant
import java.util.Date

import anorm.SqlParser.{array, binaryStream, byteArray, date, flatten, int, long, str}
import anorm.{Macro, Row, RowParser, SQL, SimpleSql, SqlParser, SqlQuery, ~}
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.platform.store.Conversions.{
  contractId,
  eventId,
  identifier,
  instant,
  ledgerString,
  offset,
}
import com.daml.lf.data.Ref.PackageId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.common.MismatchException
import com.daml.platform.store.Conversions
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.appendonlydao.JdbcLedgerDao.{acceptType, rejectType}
import com.daml.platform.store.backend.{ParameterStorageBackend, StorageBackend}
import com.daml.platform.store.backend.StorageBackend.RawTransactionEvent
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry}
import com.daml.scalautil.Statement.discard
import scalaz.syntax.tag._

private[backend] trait CommonStorageBackend[DB_BATCH] extends StorageBackend[DB_BATCH] {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)
  // Ingestion

  private val SQL_DELETE_OVERSPILL_ENTRIES: List[SqlQuery] =
    List(
      SQL("DELETE FROM configuration_entries WHERE ledger_offset > {ledger_offset}"),
      SQL("DELETE FROM package_entries WHERE ledger_offset > {ledger_offset}"),
      SQL("DELETE FROM packages WHERE ledger_offset > {ledger_offset}"),
      SQL("DELETE FROM participant_command_completions WHERE completion_offset > {ledger_offset}"),
      SQL("DELETE FROM participant_events_divulgence WHERE event_offset > {ledger_offset}"),
      SQL("DELETE FROM participant_events_create WHERE event_offset > {ledger_offset}"),
      SQL("DELETE FROM participant_events_consuming_exercise WHERE event_offset > {ledger_offset}"),
      SQL(
        "DELETE FROM participant_events_non_consuming_exercise WHERE event_offset > {ledger_offset}"
      ),
      SQL("DELETE FROM party_entries WHERE ledger_offset > {ledger_offset}"),
    )

  def queryStrategy: QueryStrategy

  override def initializeIngestion(
      connection: Connection
  ): Option[ParameterStorageBackend.LedgerEnd] = {
    val result = ledgerEnd(connection)

    result.foreach { existingLedgerEnd =>
      SQL_DELETE_OVERSPILL_ENTRIES.foreach { query =>
        import com.daml.platform.store.Conversions.OffsetToStatement
        query
          .on("ledger_offset" -> existingLedgerEnd.lastOffset)
          .execute()(connection)
        ()
      }
    }

    result
  }

  // Parameters

  private val SQL_UPDATE_LEDGER_END = SQL(
    """
      |UPDATE
      |  parameters
      |SET
      |  ledger_end = {ledger_end},
      |  ledger_end_sequential_id = {ledger_end_sequential_id}
      |""".stripMargin
  )

  override def updateLedgerEnd(
      ledgerEnd: ParameterStorageBackend.LedgerEnd
  )(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL_UPDATE_LEDGER_END
      .on("ledger_end" -> ledgerEnd.lastOffset)
      .on("ledger_end_sequential_id" -> ledgerEnd.lastEventSeqId)
      .execute()(connection)
    ()
  }

  private val SQL_GET_LEDGER_END = SQL(
    """
      |SELECT
      |  ledger_end,
      |  ledger_end_sequential_id
      |FROM
      |  parameters
      |
      |""".stripMargin
  )

  override def ledgerEnd(connection: Connection): Option[ParameterStorageBackend.LedgerEnd] =
    SQL_GET_LEDGER_END.as(LedgerEndParser.singleOpt)(connection).flatten

  private val TableName: String = "parameters"
  private val LedgerIdColumnName: String = "ledger_id"
  private val ParticipantIdColumnName: String = "participant_id"
  private val LedgerEndColumnName: String = "ledger_end"
  private val LedgerEndSequentialIdColumnName: String = "ledger_end_sequential_id"

  private val LedgerIdParser: RowParser[LedgerId] =
    ledgerString(LedgerIdColumnName).map(LedgerId(_))

  private val ParticipantIdParser: RowParser[ParticipantId] =
    Conversions.participantId(ParticipantIdColumnName).map(ParticipantId(_))

  private val LedgerEndOffsetParser: RowParser[Option[Offset]] =
    offset(LedgerEndColumnName).?

  private val LedgerEndSequentialIdParser: RowParser[Option[Long]] =
    long(LedgerEndSequentialIdColumnName).?

  private val LedgerIdentityParser: RowParser[ParameterStorageBackend.IdentityParams] =
    LedgerIdParser ~ ParticipantIdParser map { case ledgerId ~ participantId =>
      ParameterStorageBackend.IdentityParams(ledgerId, participantId)
    }

  private val LedgerEndParser: RowParser[Option[ParameterStorageBackend.LedgerEnd]] =
    LedgerEndOffsetParser ~ LedgerEndSequentialIdParser map {
      case Some(lastOffset) ~ Some(lastEventSequentialId) =>
        Some(ParameterStorageBackend.LedgerEnd(lastOffset, lastEventSequentialId))
      case _ =>
        // Note: offset and event sequential id are always written together.
        // Cases where only one of them is defined are not handled here.
        None
    }

  override def initializeParameters(
      params: ParameterStorageBackend.IdentityParams
  )(connection: Connection)(implicit loggingContext: LoggingContext): Unit = {
    // Note: this method is the only one that inserts a row into the parameters table
    val previous = ledgerIdentity(connection)
    val ledgerId = params.ledgerId
    val participantId = params.participantId
    previous match {
      case None =>
        logger.info(
          s"Initializing new database for ledgerId '${params.ledgerId}' and participantId '${params.participantId}'"
        )
        discard(
          SQL"insert into #$TableName(#$LedgerIdColumnName, #$ParticipantIdColumnName) values(${ledgerId.unwrap}, ${participantId.unwrap: String})"
            .execute()(connection)
        )
      case Some(ParameterStorageBackend.IdentityParams(`ledgerId`, `participantId`)) =>
        logger.info(
          s"Found existing database for ledgerId '${params.ledgerId}' and participantId '${params.participantId}'"
        )
      case Some(ParameterStorageBackend.IdentityParams(existing, _))
          if existing != params.ledgerId =>
        logger.error(
          s"Found existing database with mismatching ledgerId: existing '$existing', provided '${params.ledgerId}'"
        )
        throw MismatchException.LedgerId(
          existing = existing,
          provided = params.ledgerId,
        )
      case Some(ParameterStorageBackend.IdentityParams(_, existing)) =>
        logger.error(
          s"Found existing database with mismatching participantId: existing '$existing', provided '${params.participantId}'"
        )
        throw MismatchException.ParticipantId(
          existing = existing,
          provided = params.participantId,
        )
    }
  }

  override def ledgerIdentity(
      connection: Connection
  ): Option[ParameterStorageBackend.IdentityParams] =
    SQL"select #$LedgerIdColumnName, #$ParticipantIdColumnName from #$TableName"
      .as(LedgerIdentityParser.singleOpt)(connection)

  private val SQL_UPDATE_MOST_RECENT_PRUNING =
    SQL("""
        |update parameters set participant_pruned_up_to_inclusive={pruned_up_to_inclusive}
        |where participant_pruned_up_to_inclusive < {pruned_up_to_inclusive} or participant_pruned_up_to_inclusive is null
        |""".stripMargin)

  private val SQL_UPDATE_MOST_RECENT_PRUNING_INCLUDING_ALL_DIVULGED_CONTRACTS =
    SQL("""
        |update parameters set participant_all_divulged_contracts_pruned_up_to_inclusive={prune_all_divulged_contracts_up_to_inclusive}
        |where participant_pruned_up_to_inclusive < {prune_all_divulged_contracts_up_to_inclusive} or participant_all_divulged_contracts_pruned_up_to_inclusive is null
        |""".stripMargin)

  def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL_UPDATE_MOST_RECENT_PRUNING
      .on("pruned_up_to_inclusive" -> prunedUpToInclusive)
      .execute()(connection)
    ()
  }

  def updatePrunedAllDivulgedContractsUpToInclusive(
      prunedUpToInclusive: Offset
  )(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement

    SQL_UPDATE_MOST_RECENT_PRUNING_INCLUDING_ALL_DIVULGED_CONTRACTS
      .on("prune_all_divulged_contracts_up_to_inclusive" -> prunedUpToInclusive)
      .execute()(connection)
    ()
  }

  private val SQL_SELECT_MOST_RECENT_PRUNING = SQL(
    "select participant_pruned_up_to_inclusive from parameters"
  )

  def prunedUptoInclusive(connection: Connection): Option[Offset] =
    SQL_SELECT_MOST_RECENT_PRUNING
      .as(offset("participant_pruned_up_to_inclusive").?.single)(connection)

  // Configurations

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
      |    ({startExclusive} is null or ledger_offset>{startExclusive}) and
      |    ledger_offset <= {endInclusive} and
      |    parameters.ledger_end >= ledger_offset
      |  order by ledger_offset asc
      |  offset {queryOffset} rows
      |  fetch next {pageSize} rows only
      |  """.stripMargin
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
       |  fetch next 1 row only""".stripMargin
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

  def ledgerConfiguration(connection: Connection): Option[(Offset, Configuration)] =
    SQL_GET_LATEST_CONFIGURATION_ENTRY
      .on()
      .asVectorOf(configurationEntryParser)(connection)
      .collectFirst { case (offset, ConfigurationEntry.Accepted(_, configuration)) =>
        offset -> configuration
      }

  def configurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, ConfigurationEntry)] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL_GET_CONFIGURATION_ENTRIES
      .on(
        "startExclusive" -> startExclusive,
        "endInclusive" -> endInclusive,
        "pageSize" -> pageSize,
        "queryOffset" -> queryOffset,
      )
      .asVectorOf(configurationEntryParser)(connection)
  }

  // Packages

  private val SQL_SELECT_PACKAGES =
    SQL(
      """select packages.package_id, packages.source_description, packages.known_since, packages.package_size
        |from packages, parameters
        |where packages.ledger_offset <= parameters.ledger_end
        |""".stripMargin
    )

  private case class ParsedPackageData(
      packageId: String,
      sourceDescription: Option[String],
      size: Long,
      knownSince: Date,
  )

  private val PackageDataParser: RowParser[ParsedPackageData] =
    Macro.parser[ParsedPackageData](
      "package_id",
      "source_description",
      "package_size",
      "known_since",
    )

  def lfPackages(connection: Connection): Map[PackageId, PackageDetails] =
    SQL_SELECT_PACKAGES
      .as(PackageDataParser.*)(connection)
      .map(d =>
        PackageId.assertFromString(d.packageId) -> PackageDetails(
          d.size,
          d.knownSince.toInstant,
          d.sourceDescription,
        )
      )
      .toMap

  private val SQL_SELECT_PACKAGE =
    SQL("""select packages.package
          |from packages, parameters
          |where package_id = {package_id}
          |and packages.ledger_offset <= parameters.ledger_end
          |""".stripMargin)

  def lfArchive(packageId: PackageId)(connection: Connection): Option[Array[Byte]] = {
    import com.daml.platform.store.Conversions.packageIdToStatement
    SQL_SELECT_PACKAGE
      .on(
        "package_id" -> packageId
      )
      .as[Option[Array[Byte]]](SqlParser.byteArray("package").singleOpt)(connection)
  }

  private val SQL_GET_PACKAGE_ENTRIES = SQL(
    """select * from package_entries
      |where ({startExclusive} is null or ledger_offset>{startExclusive})
      |and ledger_offset<={endInclusive}
      |order by ledger_offset asc
      |offset {queryOffset} rows
      |fetch next {pageSize} rows only""".stripMargin
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

  def packageEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, PackageLedgerEntry)] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL_GET_PACKAGE_ENTRIES
      .on(
        "startExclusive" -> startExclusive,
        "endInclusive" -> endInclusive,
        "pageSize" -> pageSize,
        "queryOffset" -> queryOffset,
      )
      .asVectorOf(packageEntryParser)(connection)
  }

  // Deduplication

  private val SQL_SELECT_COMMAND = SQL("""
                                         |select deduplicate_until
                                         |from participant_command_submissions
                                         |where deduplication_key = {deduplicationKey}
    """.stripMargin)

  private case class ParsedCommandData(deduplicateUntil: Instant)

  private val CommandDataParser: RowParser[ParsedCommandData] =
    Macro.parser[ParsedCommandData](
      "deduplicate_until"
    )

  def deduplicatedUntil(deduplicationKey: String)(connection: Connection): Instant =
    SQL_SELECT_COMMAND
      .on("deduplicationKey" -> deduplicationKey)
      .as(CommandDataParser.single)(connection)
      .deduplicateUntil

  private val SQL_DELETE_EXPIRED_COMMANDS = SQL("""
                                                  |delete from participant_command_submissions
                                                  |where deduplicate_until < {currentTime}
    """.stripMargin)

  def removeExpiredDeduplicationData(currentTime: Instant)(connection: Connection): Unit = {
    SQL_DELETE_EXPIRED_COMMANDS
      .on("currentTime" -> currentTime)
      .execute()(connection)
    ()
  }

  private val SQL_DELETE_COMMAND = SQL("""
                                         |delete from participant_command_submissions
                                         |where deduplication_key = {deduplicationKey}
    """.stripMargin)

  def stopDeduplicatingCommand(deduplicationKey: String)(connection: Connection): Unit = {
    SQL_DELETE_COMMAND
      .on("deduplicationKey" -> deduplicationKey)
      .execute()(connection)
    ()
  }

  // Completions

  def pruneCompletions(
      pruneUpToInclusive: Offset
  )(connection: Connection, loggingContext: LoggingContext): Unit = {
    pruneWithLogging(queryDescription = "Command completions pruning") {
      import com.daml.platform.store.Conversions.OffsetToStatement
      SQL"delete from participant_command_completions where completion_offset <= $pruneUpToInclusive"
    }(connection, loggingContext)
  }

  // Events

  def pruneEvents(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(connection: Connection, loggingContext: LoggingContext): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement

    if (pruneAllDivulgedContracts) {
      pruneWithLogging(queryDescription = "All retroactive divulgence events pruning") {
        SQL"""
          -- Retroactive divulgence events
          delete from participant_events_divulgence delete_events
          where delete_events.event_offset <= $pruneUpToInclusive
            or delete_events.event_offset is null
          """
      }(connection, loggingContext)
    } else {
      pruneWithLogging(queryDescription = "Archived retroactive divulgence events pruning") {
        SQL"""
          -- Retroactive divulgence events (only for contracts archived before the specified offset)
          delete from participant_events_divulgence delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive
            and exists (
              select 1 from participant_events_consuming_exercise archive_events
              where
                archive_events.event_offset <= $pruneUpToInclusive and
                archive_events.contract_id = delete_events.contract_id
            )"""
      }(connection, loggingContext)
    }

    pruneWithLogging(queryDescription = "Create events pruning") {
      SQL"""
          -- Create events (only for contracts archived before the specified offset)
          delete from participant_events_create delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive and
            exists (
              SELECT 1 FROM participant_events_consuming_exercise archive_events
              WHERE
                archive_events.event_offset <= $pruneUpToInclusive AND
                archive_events.contract_id = delete_events.contract_id
            )"""
    }(connection, loggingContext)

    if (pruneAllDivulgedContracts) {
      val pruneAfterClause =
        participantAllDivulgedContractsPrunedUpToInclusive(connection) match {
          case Some(pruneAfter) => cSQL"and event_offset > $pruneAfter"
          case None => cSQL""
        }

      pruneWithLogging(queryDescription = "Immediate divulgence events pruning") {
        SQL"""
            -- Immediate divulgence pruning
            delete from participant_events_create c
            where event_offset <= $pruneUpToInclusive
            -- Only prune create events which did not have a locally hosted party before their creation offset
            and not exists (
              select 1
              from party_entries p
              where p.typ = 'accept'
              and p.ledger_offset <= c.event_offset
              and #${queryStrategy.isTrue("p.is_local")}
              and #${queryStrategy.arrayContains("c.flat_event_witnesses", "p.party")}
            )
            $pruneAfterClause
         """
      }(connection, loggingContext)
    }

    pruneWithLogging(queryDescription = "Exercise (consuming) events pruning") {
      SQL"""
          -- Exercise events (consuming)
          delete from participant_events_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive"""
    }(connection, loggingContext)

    pruneWithLogging(queryDescription = "Exercise (non-consuming) events pruning") {
      SQL"""
          -- Exercise events (non-consuming)
          delete from participant_events_non_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive"""
    }(connection, loggingContext)
  }

  private def participantAllDivulgedContractsPrunedUpToInclusive(
      connection: Connection
  ): Option[Offset] =
    SQL"select participant_all_divulged_contracts_pruned_up_to_inclusive from parameters"
      .as(offset("participant_all_divulged_contracts_pruned_up_to_inclusive").?.single)(
        connection
      )

  private def pruneWithLogging(queryDescription: String)(query: SimpleSql[Row])(
      connection: Connection,
      loggingContext: LoggingContext,
  ): Unit = {
    val deletedRows = query.executeUpdate()(connection)
    logger.info(s"$queryDescription finished: deleted $deletedRows rows.")(loggingContext)
  }

  private val rawTransactionEventParser: RowParser[RawTransactionEvent] = {
    import com.daml.platform.store.Conversions.ArrayColumnToStringArray.arrayColumnToStringArray
    (int("event_kind") ~
      str("transaction_id") ~
      int("node_index") ~
      str("command_id").? ~
      str("workflow_id").? ~
      eventId("event_id") ~
      contractId("contract_id") ~
      identifier("template_id").? ~
      instant("ledger_effective_time").? ~
      array[String]("create_signatories").? ~
      array[String]("create_observers").? ~
      str("create_agreement_text").? ~
      binaryStream("create_key_value").? ~
      int("create_key_value_compression").? ~
      binaryStream("create_argument").? ~
      int("create_argument_compression").? ~
      array[String]("tree_event_witnesses") ~
      array[String]("flat_event_witnesses") ~
      array[String]("submitters").? ~
      str("exercise_choice").? ~
      binaryStream("exercise_argument").? ~
      int("exercise_argument_compression").? ~
      binaryStream("exercise_result").? ~
      int("exercise_result_compression").? ~
      array[String]("exercise_actors").? ~
      array[String]("exercise_child_event_ids").? ~
      long("event_sequential_id") ~
      offset("event_offset")).map {
      case eventKind ~ transactionId ~ nodeIndex ~ commandId ~ workflowId ~ eventId ~ contractId ~ templateId ~ ledgerEffectiveTime ~ createSignatories ~
          createObservers ~ createAgreementText ~ createKeyValue ~ createKeyCompression ~
          createArgument ~ createArgumentCompression ~ treeEventWitnesses ~ flatEventWitnesses ~ submitters ~ exerciseChoice ~
          exerciseArgument ~ exerciseArgumentCompression ~ exerciseResult ~ exerciseResultCompression ~ exerciseActors ~
          exerciseChildEventIds ~ eventSequentialId ~ offset =>
        RawTransactionEvent(
          eventKind,
          transactionId,
          nodeIndex,
          commandId,
          workflowId,
          eventId,
          contractId,
          templateId,
          ledgerEffectiveTime,
          createSignatories,
          createObservers,
          createAgreementText,
          createKeyValue,
          createKeyCompression,
          createArgument,
          createArgumentCompression,
          treeEventWitnesses.toSet,
          flatEventWitnesses.toSet,
          submitters.map(_.toSet).getOrElse(Set.empty),
          exerciseChoice,
          exerciseArgument,
          exerciseArgumentCompression,
          exerciseResult,
          exerciseResultCompression,
          exerciseActors,
          exerciseChildEventIds,
          eventSequentialId,
          offset,
        )
    }
  }

  def rawEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[RawTransactionEvent] =
    SQL"""
       SELECT
           event_kind,
           transaction_id,
           node_index,
           command_id,
           workflow_id,
           event_id,
           contract_id,
           template_id,
           ledger_effective_time,
           create_signatories,
           create_observers,
           create_agreement_text,
           create_key_value,
           create_key_value_compression,
           create_argument,
           create_argument_compression,
           tree_event_witnesses,
           flat_event_witnesses,
           submitters,
           exercise_choice,
           exercise_argument,
           exercise_argument_compression,
           exercise_result,
           exercise_result_compression,
           exercise_actors,
           exercise_child_event_ids,
           event_sequential_id,
           event_offset
       FROM
           participant_events
       WHERE
           event_sequential_id > $startExclusive
           and event_sequential_id <= $endInclusive
           and event_kind != 0
       ORDER BY event_sequential_id ASC"""
      .asVectorOf(rawTransactionEventParser)(connection)

  protected def exe(statement: String): Connection => Unit = { connection =>
    val stmnt = connection.createStatement()
    try {
      stmnt.execute(statement)
      ()
    } finally {
      stmnt.close()
    }
  }
}
