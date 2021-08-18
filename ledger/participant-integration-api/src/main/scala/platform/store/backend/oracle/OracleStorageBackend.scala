// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import anorm.SqlParser.get
import anorm.{NamedParameter, SQL, SqlStringInterpolation}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.lf.data.Ref
import com.daml.platform.store.appendonlydao.events.{ContractId, Key}
import com.daml.platform.store.backend.common.{AppendOnlySchema, CommonStorageBackend, EventStorageBackendTemplate, EventStrategy, InitHookDataSourceProxy, TemplatedStorageBackend}
import com.daml.platform.store.backend.{DBLockStorageBackend, DataSourceStorageBackend, DbDto, StorageBackend, common}

import java.sql.Connection
import java.time.Instant
import com.daml.ledger.offset.Offset
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.logging.LoggingContext
import com.daml.platform.store.Conversions.contractId

import javax.sql.DataSource

private[backend] object OracleStorageBackend
    extends StorageBackend[AppendOnlySchema.Batch]
    with CommonStorageBackend[AppendOnlySchema.Batch]
    with EventStorageBackendTemplate {

  override def reset(connection: Connection): Unit =
    List(
      "truncate table configuration_entries cascade",
      "truncate table package_entries cascade",
      "truncate table parameters cascade",
      "truncate table participant_command_completions cascade",
      "truncate table participant_command_submissions cascade",
      "truncate table participant_events_divulgence cascade",
      "truncate table participant_events_create cascade",
      "truncate table participant_events_consuming_exercise cascade",
      "truncate table participant_events_non_consuming_exercise cascade",
      "truncate table parties cascade",
      "truncate table party_entries cascade",
    ).map(SQL(_)).foreach(_.execute()(connection))

  override def duplicateKeyError: String = "unique constraint"

  val SQL_INSERT_COMMAND: String =
    """merge into participant_command_submissions pcs
      |using dual
      |on (pcs.deduplication_key ={deduplicationKey})
      |when matched then
      |  update set pcs.deduplicate_until={deduplicateUntil}
      |  where pcs.deduplicate_until < {submittedAt}
      |when not matched then
      | insert (pcs.deduplication_key, pcs.deduplicate_until)
      |  values ({deduplicationKey}, {deduplicateUntil})""".stripMargin

  def upsertDeduplicationEntry(
      key: String,
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(connection: Connection): Int =
    SQL(SQL_INSERT_COMMAND)
      .on(
        "deduplicationKey" -> key,
        "submittedAt" -> submittedAt,
        "deduplicateUntil" -> deduplicateUntil,
      )
      .executeUpdate()(connection)

  override def batch(dbDtos: Vector[DbDto]): AppendOnlySchema.Batch =
    OracleSchema.schema.prepareData(dbDtos)

  override def insertBatch(connection: Connection, batch: AppendOnlySchema.Batch): Unit =
    OracleSchema.schema.executeUpdate(batch, connection)

  def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(connection: Connection): List[CompletionStreamResponse] =
    TemplatedStorageBackend.commandCompletions(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      applicationId = applicationId,
      submittersInPartiesClause = arrayIntersectionWhereClauseUseCase1(parties),
    )(connection)

  def activeContractWithArgument(readers: Set[Ref.Party], contractId: ContractId)(
      connection: Connection
  ): Option[StorageBackend.RawContract] =
    TemplatedStorageBackend.activeContractWithArgument(
      participantTreeWitnessEventsWhereClause = arrayIntersectionWhereClauseParticipantEvents(readers),
      divulgenceEventsTreeWitnessWhereClause = arrayIntersectionWhereClauseDivulgence(readers),
      contractId = contractId,
    )(connection)

  def activeContractWithoutArgument(readers: Set[Ref.Party], contractId: ContractId)(
      connection: Connection
  ): Option[String] =
    TemplatedStorageBackend.activeContractWithoutArgument(
      treeEventWitnessesWhereClause = arrayIntersectionWhereClauseParticipantEvents(readers),
      contractId = contractId,
    )(connection)

  def contractKey(readers: Set[Ref.Party], key: Key)(
      connection: Connection
  ): Option[ContractId] =
    contractKey(
      flatEventWitnesses = columnPrefix =>
        arrayIntersectionWhereClause(s"$columnPrefix.flat_event_witnesses", readers),
      key = key,
      readers
    )(connection)

  private def contractKey(flatEventWitnesses: String => String, key: Key, readers: Set[Ref.Party])(
    connection: Connection
  ): Option[ContractId] = {
    import com.daml.platform.store.Conversions.HashToStatement
    SQL"""
  WITH last_contract_key_create AS (
         SELECT participant_events.*
           FROM participant_events, parameters
          WHERE event_kind = 10 -- create
            AND create_key_hash = ${key.hash}
            AND event_sequential_id <= parameters.ledger_end_sequential_id
                -- do NOT check visibility here, as otherwise we do not abort the scan early
          ORDER BY event_sequential_id DESC
          FETCH NEXT 1 ROW ONLY
       )
  SELECT contract_id
    FROM last_contract_key_create -- creation only, as divulged contracts cannot be fetched by key
  WHERE #${flatEventWitnesses("last_contract_key_create")} -- check visibility only here
    AND NOT EXISTS       -- check no archival visible
         (SELECT 1
            FROM participant_events, parameters
           WHERE event_kind = 20 -- consuming exercise
             AND event_sequential_id <= parameters.ledger_end_sequential_id
             AND #${arrayIntersectionWhereClause("flat_event_witness", "PARTICIPANT_EVENTS_FLAT_WITNESS", "EVENT_SEQUENTIAL_ID", "participant_events", readers)}
             AND contract_id = last_contract_key_create.contract_id
         )
       """.as(contractId("contract_id").singleOpt)(connection)
  }

  object OracleEventStrategy extends EventStrategy {

    def arrayIntersectionClause(
        columnName: String,
        columnPrefix: String,
        parties: Set[Ref.Party],
        paramNamePostfix: String,
    ): (String, List[NamedParameter]) = {
      val (viewColumnName, viewName, idColumn) = columnName.toLowerCase match {
        case "flat_event_witnesses" => ("flat_event_witness", "PARTICIPANT_EVENTS_FLAT_WITNESS", "EVENT_SEQUENTIAL_ID")
        case "tree_event_witnesses" => ("tree_event_witness", "PARTICIPANT_EVENTS_TREE_WITNESS", "EVENT_SEQUENTIAL_ID")
        case "submitters" => ("submitter", "PARTICIPANT_EVENTS_SUBMITTERS", "EVENT_SEQUENTIAL_ID")
      }
      val colPrefix = if(columnPrefix == "") "PARTICIPANT_EVENTS" else columnPrefix
      (
        s"""$colPrefix.$idColumn in (select v.$idColumn from $viewName v
           |        where v.$viewColumnName in ({parties$paramNamePostfix}))""".stripMargin,
        List(s"parties$paramNamePostfix" -> parties.map(_.toString)),
      )
    }

    override def filteredEventWitnessesClause(
        witnessesColumnName: String,
        parties: Set[Ref.Party],
    ): (String, List[NamedParameter]) =
      if (parties.size == 1)
        (
          "(json_array({singlePartyfewc}))",
          List[NamedParameter]("singlePartyfewc" -> parties.head.toString),
        )
      else
        (
          s"""(select json_arrayagg(value) from (select value
             |from json_table($witnessesColumnName, '$$[*]' columns (value PATH '$$'))
             |where value IN ({partiesfewc})))
             |""".stripMargin,
          List[NamedParameter]("partiesfewc" -> parties.map(_.toString)),
        )

    override def submittersArePartiesClause(
        submittersColumnName: String,
        parties: Set[Ref.Party],
        columnPrefix: String
    ): (String, List[NamedParameter]) = {
      val (clause, params) = arrayIntersectionClause(submittersColumnName, columnPrefix, parties, "sapc")
      (s"($clause)", params)
    }

    override def witnessesWhereClause(
        witnessesColumnName: String,
        filterParams: FilterParams,
        columnPrefix: String
    ): (String, List[NamedParameter]) = {
      val (wildCardClause, wildCardParams) = filterParams.wildCardParties match {
        case wildCardParties if wildCardParties.isEmpty => (Nil, Nil)
        case wildCardParties =>
          val (clause, params) =
            arrayIntersectionClause(witnessesColumnName, columnPrefix, wildCardParties, "wcwwc")
          (
            List(s"($clause)"),
            params,
          )
      }
      val (partiesTemplatesClauses, partiesTemplatesParams) =
        filterParams.partiesAndTemplates.iterator.zipWithIndex
          .map { case ((parties, templateIds), index) =>
            val (clause, params) =
              arrayIntersectionClause(witnessesColumnName, columnPrefix, parties, s"ptwwc$index")
            (
              s"( ($clause) AND (template_id IN ({templateIdsArraywwc$index})) )",
              List[NamedParameter](
                s"templateIdsArraywwc$index" -> templateIds.map(_.toString)
              ) ::: params,
            )
          }
          .toList
          .unzip
      val res = (
        (wildCardClause ::: partiesTemplatesClauses).mkString("(", " OR ", ")"),
        wildCardParams ::: partiesTemplatesParams.flatten,
      )
      println("printing out res", res)
      res
    }

    override def columnEqualityBoolean(column: String, value: String): String =
      s"""case when ($column = $value) then 1 else 0 end"""
  }

  override def eventStrategy: common.EventStrategy = OracleEventStrategy

  // TODO FIXME: confirm this works for oracle
  def maxEventSeqIdForOffset(offset: Offset)(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
    // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
    // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
    SQL"select max(event_sequential_id) from participant_events where event_offset <= $offset group by event_offset order by event_offset desc #${limitClause(Some(1))}"
      .as(get[Long](1).singleOpt)(connection)
  }

  // TODO append-only: this seems to be the same for all db backends, let's unify
  private def limitClause(to: Option[Int]): String =
    to.map(to => s"fetch next $to rows only").getOrElse("")

  // TODO append-only: remove as part of ContractStorageBackend consolidation, use the data-driven one

  private def arrayIntersectionWhereClauseUseCase1(parties: Set[Ref.Party]): String = {
    arrayIntersectionWhereClause(
      "submitter",
      "participant_command_completions_submitters",
      "COMPLETION_OFFSET",
      "participant_command_completions",
      parties)
  }

  private def arrayIntersectionWhereClauseDivulgence(parties: Set[Ref.Party]): String = {
    arrayIntersectionWhereClause(
      "tree_event_witness",
      "PARTICIPANT_EVENTS_TREE_WITNESS",
      "EVENT_SEQUENTIAL_ID",
      "divulgence_events",
      parties)
  }
  private def arrayIntersectionWhereClauseParticipantEvents(parties: Set[Ref.Party]): String = {
    arrayIntersectionWhereClause(
      "tree_event_witness",
      "PARTICIPANT_EVENTS_TREE_WITNESS",
      "EVENT_SEQUENTIAL_ID",
      "participant_events",
      parties)

  }

    private def arrayIntersectionWhereClause(viewColumn: String, view: String, idColumn: String, table: String, parties: Set[Ref.Party]): String = {
    println("ARRAY INTERSECTION WHERE CLAUSE", table)
    if(parties.isEmpty)
      "false"
    else {
      s"""$table.$idColumn in (select v.$idColumn from $view v
        |        where v.$viewColumn in (${parties.map(p=> s"'$p'").mkString(",")}))""".stripMargin
    }
  }

  private def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Ref.Party]): String = {
    println("OLD ARRAY INTERSECTION WHERE CLAUSE")
    if (parties.isEmpty)
      "false"
    else {
      val NumCharsBetweenParties = 3
      val NumExtraChars = 20
      val OracleMaxStringLiteralLength = 4000

      val groupedParties =
        parties.foldLeft((List.empty[List[String]], 0))({ case ((prev, currentLength), party) =>
          if (
            currentLength + party.length + NumCharsBetweenParties > OracleMaxStringLiteralLength
          ) {
            (List(party) :: prev, party.length + NumExtraChars)
          } else {
            prev match {
              case h :: tail =>
                ((party :: h) :: tail, currentLength + party.length + NumCharsBetweenParties)
              case Nil => (List(party) :: Nil, party.length + NumExtraChars)
            }
          }
        })
      "(" + groupedParties._1
        .map { listOfParties =>
          s"""JSON_EXISTS($arrayColumn, '$$[*]?(@ in ("${listOfParties.mkString("""","""")}"))')"""
        }
        .mkString(" OR ") + ")"
    }
  }

  override def createDataSource(
      jdbcUrl: String,
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      connectionInitHook: Option[Connection => Unit],
  )(implicit loggingContext: LoggingContext): DataSource = {
    val oracleDataSource = new oracle.jdbc.pool.OracleDataSource
    oracleDataSource.setURL(jdbcUrl)
    InitHookDataSourceProxy(oracleDataSource, connectionInitHook.toList)
  }

  override def tryAcquire(
      lockId: DBLockStorageBackend.LockId,
      lockMode: DBLockStorageBackend.LockMode,
  )(connection: Connection): Option[DBLockStorageBackend.Lock] = {
    val oracleLockMode = lockMode match {
      case DBLockStorageBackend.LockMode.Exclusive => "6" // "DBMS_LOCK.x_mode"
      case DBLockStorageBackend.LockMode.Shared => "4" // "DBMS_LOCK.s_mode"
    }
    SQL"""
          SELECT DBMS_LOCK.REQUEST(
            id => ${oracleIntLockId(lockId)},
            lockmode => #$oracleLockMode,
            timeout => 0
          ) FROM DUAL"""
      .as(get[Int](1).single)(connection) match {
      case 0 => Some(DBLockStorageBackend.Lock(lockId, lockMode))
      case 1 => None
      case 2 => throw new Exception("DBMS_LOCK.REQUEST Error 2: Acquiring lock caused a deadlock!")
      case 3 => throw new Exception("DBMS_LOCK.REQUEST Error 3: Parameter error as acquiring lock")
      case 4 => Some(DBLockStorageBackend.Lock(lockId, lockMode))
      case 5 =>
        throw new Exception("DBMS_LOCK.REQUEST Error 5: Illegal lock handle as acquiring lock")
      case unknown => throw new Exception(s"Invalid result from DBMS_LOCK.REQUEST: $unknown")
    }
  }

  override def release(lock: DBLockStorageBackend.Lock)(connection: Connection): Boolean = {
    SQL"""
          SELECT DBMS_LOCK.RELEASE(
            id => ${oracleIntLockId(lock.lockId)}
          ) FROM DUAL"""
      .as(get[Int](1).single)(connection) match {
      case 0 => true
      case 3 => throw new Exception("DBMS_LOCK.RELEASE Error 3: Parameter error as releasing lock")
      case 4 => throw new Exception("DBMS_LOCK.RELEASE Error 4: Trying to release not-owned lock")
      case 5 =>
        throw new Exception("DBMS_LOCK.RELEASE Error 5: Illegal lock handle as releasing lock")
      case unknown => throw new Exception(s"Invalid result from DBMS_LOCK.RELEASE: $unknown")
    }
  }

  case class OracleLockId(id: Int) extends DBLockStorageBackend.LockId {
    // respecting Oracle limitations: https://docs.oracle.com/cd/B19306_01/appdev.102/b14258/d_lock.htm#ARPLS021
    assert(id >= 0)
    assert(id <= 1073741823)
  }

  private def oracleIntLockId(lockId: DBLockStorageBackend.LockId): Int =
    lockId match {
      case OracleLockId(id) => id
      case unknown =>
        throw new Exception(
          s"LockId $unknown not supported. Probable cause: LockId was created by a different StorageBackend"
        )
    }

  override def lock(id: Int): DBLockStorageBackend.LockId = OracleLockId(id)

  override def dbLockSupported: Boolean = true
}
