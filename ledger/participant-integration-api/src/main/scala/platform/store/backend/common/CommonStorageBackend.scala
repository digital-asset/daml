// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.time.Instant
import java.util.Date

import anorm.SqlParser.{array, binaryStream, bool, byteArray, date, flatten, int, long, str}
import anorm.{Macro, RowParser, SQL, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain.{LedgerId, ParticipantId, PartyDetails}
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
  party,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.common.MismatchException
import com.daml.platform.store.Conversions
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.appendonlydao.JdbcLedgerDao.{acceptType, rejectType}
import com.daml.platform.store.backend.StorageBackend
import com.daml.platform.store.backend.StorageBackend.{OptionalIdentityParams, RawTransactionEvent}
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.scalautil.Statement.discard

import scala.collection.mutable
import scalaz.syntax.tag._

private[backend] trait CommonStorageBackend[DB_BATCH] extends StorageBackend[DB_BATCH] {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  // Ingestion

  private val preparedDeleteIngestionOverspillEntries: List[String] =
    List(
      "DELETE FROM configuration_entries WHERE ledger_offset > ?",
      "DELETE FROM package_entries WHERE ledger_offset > ?",
      "DELETE FROM packages WHERE ledger_offset > ?",
      "DELETE FROM participant_command_completions WHERE completion_offset > ?",
      "DELETE FROM participant_events_divulgence WHERE event_offset > ?",
      "DELETE FROM participant_events_create WHERE event_offset > ?",
      "DELETE FROM participant_events_consuming_exercise WHERE event_offset > ?",
      "DELETE FROM participant_events_non_consuming_exercise WHERE event_offset > ?",
      "DELETE FROM parties WHERE ledger_offset > ?",
      "DELETE FROM party_entries WHERE ledger_offset > ?",
    )

  override def initializeIngestion(connection: Connection): StorageBackend.OptionalLedgerEnd = {
    val result @ StorageBackend.OptionalLedgerEnd(offset, _) = ledgerEnd(connection)

    offset.foreach { existingOffset =>
      preparedDeleteIngestionOverspillEntries.foreach { preparedStatementString =>
        val preparedStatement = connection.prepareStatement(preparedStatementString)
        preparedStatement.setString(1, existingOffset.toHexString)
        preparedStatement.execute()
        preparedStatement.close()
        ()
      }
    }

    result
  }

  // Parameters

  private val preparedUpdateLedgerEnd: Connection => PreparedStatement = _.prepareStatement(
    """
      |UPDATE
      |  parameters
      |SET
      |  ledger_end = ?,
      |  ledger_end_sequential_id = ?
      |
      |""".stripMargin
  )

  override def updateLedgerEnd(
      ledgerEnd: StorageBackend.LedgerEnd
  )(connection: Connection): Unit = {
    val preparedStatement = preparedUpdateLedgerEnd(connection)
    preparedStatement.setString(1, ledgerEnd.lastOffset.toHexString)
    preparedStatement.setLong(2, ledgerEnd.lastEventSeqId)
    preparedStatement.execute()
    preparedStatement.close()
    ()
  }

  override def ledgerEnd(connection: Connection): StorageBackend.OptionalLedgerEnd = {
    val queryStatement = connection.createStatement()
    val params = fetch(
      queryStatement.executeQuery(
        """
          |SELECT
          |  ledger_end,
          |  ledger_end_sequential_id
          |FROM
          |  parameters
          |
          |""".stripMargin
      )
    )(rs => {
      val rawLedgerEnd = rs.getString(1) // SQL NULL is mapped to JVM null
      val rawEventSeqId = rs.getLong(2) // SQL NULL is mapped to JVM 0
      val rawEventSeqIdIsNull = rs.wasNull()
      StorageBackend.OptionalLedgerEnd(
        lastOffset =
          if (rawLedgerEnd == null)
            None
          else
            Some(Offset.fromHexString(Ref.HexString.assertFromString(rs.getString(1)))),
        lastEventSeqId =
          if (rawEventSeqIdIsNull)
            None
          else
            Some(rawEventSeqId),
      )
    })
    queryStatement.close()
    params.headOption.getOrElse(StorageBackend.OptionalLedgerEnd(None, None))
  }

  private def fetch[T](resultSet: ResultSet)(parse: ResultSet => T): Vector[T] = {
    val buffer = mutable.ArrayBuffer.empty[T]
    while (resultSet.next()) {
      buffer += parse(resultSet)
    }
    resultSet.close()
    buffer.toVector
  }

  private val TableName: String = "parameters"
  private val LedgerIdColumnName: String = "ledger_id"
  private val ParticipantIdColumnName: String = "participant_id"

  private val LedgerIdParser: RowParser[LedgerId] =
    ledgerString(LedgerIdColumnName).map(LedgerId(_))

  private val ParticipantIdParser: RowParser[Option[ParticipantId]] =
    Conversions.participantId(ParticipantIdColumnName).map(ParticipantId(_)).?

  private val LedgerIdentityParser: RowParser[OptionalIdentityParams] =
    LedgerIdParser ~ ParticipantIdParser map { case ledgerId ~ participantId =>
      OptionalIdentityParams(Some(ledgerId), participantId)
    }

  override def initializeParameters(
      params: StorageBackend.IdentityParams
  )(connection: Connection)(implicit loggingContext: LoggingContext): Try[Unit] = {
    // Note: this method is the only one that inserts a row into the parameters table
    val previous = ledgerIdentity(connection)
    val ledgerId = params.ledgerId
    val participantId = params.participantId
    previous match {
      case StorageBackend.OptionalIdentityParams(None, _) =>
        // ledgerId is not null, this is the case where the the parameters table is empty
        logger.info(
          s"Initializing new database for ledgerId '${params.ledgerId}' and participantId '${params.participantId}'"
        )
        discard(
          SQL"insert into #$TableName(#$LedgerIdColumnName, #$ParticipantIdColumnName) values(${ledgerId.unwrap}, ${participantId.unwrap: String})"
            .execute()(connection)
        )
        Success(())
      case StorageBackend.OptionalIdentityParams(Some(`ledgerId`), None) =>
        logger.info(
          s"Found existing database for ledgerId '${params.ledgerId}', initializing participantId '${params.participantId}'"
        )
        discard(
          SQL"update #$TableName set #$ParticipantIdColumnName=${participantId.unwrap: String}"
            .execute()(connection)
        )
        Success(())
      case StorageBackend.OptionalIdentityParams(Some(`ledgerId`), Some(`participantId`)) =>
        logger.info(
          s"Found existing database for ledgerId '${params.ledgerId}' and participantId '${params.participantId}'"
        )
        Success(())
      case StorageBackend.OptionalIdentityParams(_, Some(existing))
          if existing != params.participantId =>
        logger.error(
          s"Found existing database with mismatching participantId: existing '$existing', provided '${params.participantId}'"
        )
        Failure(
          new MismatchException.ParticipantId(
            existing = existing,
            provided = params.participantId,
          )
        )
      case StorageBackend.OptionalIdentityParams(Some(existing), _) =>
        logger.error(
          s"Found existing database with mismatching ledgerId: existing '$existing', provided '${params.ledgerId}'"
        )
        Failure(
          new MismatchException.LedgerId(
            existing = existing,
            provided = params.ledgerId,
          )
        )
    }
  }

  override def ledgerIdentity(connection: Connection): StorageBackend.OptionalIdentityParams =
    SQL"select #$LedgerIdColumnName, #$ParticipantIdColumnName from #$TableName"
      .as(LedgerIdentityParser.singleOpt)(connection)
      .getOrElse(OptionalIdentityParams(None, None))

  private val SQL_UPDATE_MOST_RECENT_PRUNING = SQL("""
                                                     |update parameters set participant_pruned_up_to_inclusive={pruned_up_to_inclusive}
                                                     |where participant_pruned_up_to_inclusive < {pruned_up_to_inclusive} or participant_pruned_up_to_inclusive is null
                                                     |""".stripMargin)

  def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL_UPDATE_MOST_RECENT_PRUNING
      .on("pruned_up_to_inclusive" -> prunedUpToInclusive)
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

  // Parties

  private val SQL_GET_PARTY_ENTRIES = SQL(
    """select * from party_entries
      |where ({startExclusive} is null or ledger_offset>{startExclusive}) and ledger_offset<={endInclusive}
      |order by ledger_offset asc
      |offset {queryOffset} rows
      |fetch next {pageSize} rows only""".stripMargin
  )

  private val partyEntryParser: RowParser[(Offset, PartyLedgerEntry)] = {
    import com.daml.platform.store.Conversions.bigDecimalColumnToBoolean
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
  }

  def partyEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, PartyLedgerEntry)] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL_GET_PARTY_ENTRIES
      .on(
        "startExclusive" -> startExclusive,
        "endInclusive" -> endInclusive,
        "pageSize" -> pageSize,
        "queryOffset" -> queryOffset,
      )
      .asVectorOf(partyEntryParser)(connection)
  }

  private val SQL_SELECT_MULTIPLE_PARTIES =
    SQL(
      "select parties.party, parties.display_name, parties.ledger_offset, parties.explicit, parties.is_local from parties, parameters where party in ({parties}) and parties.ledger_offset <= parameters.ledger_end"
    )

  private case class ParsedPartyData(
      party: String,
      displayName: Option[String],
      ledgerOffset: Offset,
      explicit: Boolean,
      isLocal: Boolean,
  )

  private val PartyDataParser: RowParser[ParsedPartyData] = {
    import com.daml.platform.store.Conversions.columnToOffset
    import com.daml.platform.store.Conversions.bigDecimalColumnToBoolean
    Macro.parser[ParsedPartyData](
      "party",
      "display_name",
      "ledger_offset",
      "explicit",
      "is_local",
    )
  }

  private def constructPartyDetails(data: ParsedPartyData): PartyDetails =
    PartyDetails(Ref.Party.assertFromString(data.party), data.displayName, data.isLocal)

  def parties(parties: Seq[Ref.Party])(connection: Connection): List[PartyDetails] = {
    import com.daml.platform.store.Conversions.partyToStatement
    SQL_SELECT_MULTIPLE_PARTIES
      .on("parties" -> parties)
      .as(PartyDataParser.*)(connection)
      .map(constructPartyDetails)
  }

  private val SQL_SELECT_ALL_PARTIES =
    SQL(
      "select parties.party, parties.display_name, parties.ledger_offset, parties.explicit, parties.is_local from parties, parameters where parameters.ledger_end >= parties.ledger_offset"
    )

  def knownParties(connection: Connection): List[PartyDetails] =
    SQL_SELECT_ALL_PARTIES
      .as(PartyDataParser.*)(connection)
      .map(constructPartyDetails)

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

  def pruneCompletions(pruneUpToInclusive: Offset)(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL"delete from participant_command_completions where completion_offset <= $pruneUpToInclusive"
      .execute()(connection)
    ()
  }

  // Events

  def pruneEvents(pruneUpToInclusive: Offset)(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    List(
      SQL"""
          -- Divulgence events (only for contracts archived before the specified offset)
          delete from participant_events_divulgence delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive and
            exists (
              SELECT 1 FROM participant_events_consuming_exercise archive_events
              WHERE
                archive_events.event_offset <= $pruneUpToInclusive AND
                archive_events.contract_id = delete_events.contract_id
            )""",
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
            )""",
      SQL"""
          -- Exercise events (consuming)
          delete from participant_events_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive""",
      SQL"""
          -- Exercise events (non-consuming)
          delete from participant_events_non_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive""",
    ).foreach(_.execute()(connection))
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
