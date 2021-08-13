// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import anorm.SqlParser.get
import anorm.SQL
import com.daml.lf.data.Ref
import com.daml.platform.store.backend.common.{
  AppendOnlySchema,
  CommonStorageBackend,
  CompletionStorageBackendTemplate,
  ContractStorageBackendTemplate,
  EventStorageBackendTemplate,
  EventStrategy,
  InitHookDataSourceProxy,
  QueryStrategy,
}
import com.daml.platform.store.backend.{
  DBLockStorageBackend,
  DataSourceStorageBackend,
  DbDto,
  StorageBackend,
  common,
}
import java.sql.Connection
import java.time.Instant

import com.daml.ledger.offset.Offset
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.logging.LoggingContext
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import javax.sql.DataSource

private[backend] object OracleStorageBackend
    extends StorageBackend[AppendOnlySchema.Batch]
    with CommonStorageBackend[AppendOnlySchema.Batch]
    with EventStorageBackendTemplate
    with ContractStorageBackendTemplate
    with CompletionStorageBackendTemplate {

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

  object OracleQueryStrategy extends QueryStrategy {

    override def arrayIntersectionNonEmptyClause(
        columnName: String,
        parties: Set[Ref.Party],
    ): CompositeSql =
      cSQL"EXISTS (SELECT 1 FROM JSON_TABLE(#$columnName, '$$[*]' columns (value PATH '$$')) WHERE value IN (${parties
        .map(_.toString)}))"

    override def columnEqualityBoolean(column: String, value: String): String =
      s"""case when ($column = $value) then 1 else 0 end"""
  }

  override def queryStrategy: QueryStrategy = OracleQueryStrategy

  object OracleEventStrategy extends EventStrategy {

    override def filteredEventWitnessesClause(
        witnessesColumnName: String,
        parties: Set[Ref.Party],
    ): CompositeSql =
      if (parties.size == 1)
        cSQL"(json_array(${parties.head.toString}))"
      else
        cSQL"""
           (select json_arrayagg(value) from (select value
           from json_table(#$witnessesColumnName, '$$[*]' columns (value PATH '$$'))
           where value IN (${parties.map(_.toString)})))
           """

    override def submittersArePartiesClause(
        submittersColumnName: String,
        parties: Set[Ref.Party],
    ): CompositeSql =
      cSQL"(${OracleQueryStrategy.arrayIntersectionNonEmptyClause(submittersColumnName, parties)})"

    override def witnessesWhereClause(
        witnessesColumnName: String,
        filterParams: FilterParams,
    ): CompositeSql = {
      val wildCardClause = filterParams.wildCardParties match {
        case wildCardParties if wildCardParties.isEmpty =>
          Nil

        case wildCardParties =>
          cSQL"(${OracleQueryStrategy.arrayIntersectionNonEmptyClause(witnessesColumnName, wildCardParties)})" :: Nil
      }
      val partiesTemplatesClauses =
        filterParams.partiesAndTemplates.iterator.map { case (parties, templateIds) =>
          val clause =
            OracleQueryStrategy.arrayIntersectionNonEmptyClause(
              witnessesColumnName,
              parties,
            )
          cSQL"( ($clause) AND (template_id IN (${templateIds.map(_.toString)})) )"
        }.toList
      (wildCardClause ::: partiesTemplatesClauses).mkComposite("(", " OR ", ")")
    }
  }

  override def eventStrategy: common.EventStrategy = OracleEventStrategy

  // TODO FIXME: confirm this works for oracle
  def maxEventSeqIdForOffset(offset: Offset)(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
    // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
    // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
    val limitClause = OracleQueryStrategy.limitClause(Some(1))
    SQL"select max(event_sequential_id) from participant_events where event_offset <= $offset group by event_offset order by event_offset desc $limitClause"
      .as(get[Long](1).singleOpt)(connection)
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
