// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection
import java.time.Instant

import anorm.SQL
import anorm.SqlParser.get
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.store.appendonlydao.events.Party
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
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
import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource

private[backend] object PostgresStorageBackend
    extends StorageBackend[AppendOnlySchema.Batch]
    with CommonStorageBackend[AppendOnlySchema.Batch]
    with EventStorageBackendTemplate
    with ContractStorageBackendTemplate
    with CompletionStorageBackendTemplate {

  override def insertBatch(
      connection: Connection,
      postgresDbBatch: AppendOnlySchema.Batch,
  ): Unit =
    PGSchema.schema.executeUpdate(postgresDbBatch, connection)

  override def batch(dbDtos: Vector[DbDto]): AppendOnlySchema.Batch =
    PGSchema.schema.prepareData(dbDtos)

  private val SQL_INSERT_COMMAND: String =
    """insert into participant_command_submissions as pcs (deduplication_key, deduplicate_until)
      |values ({deduplicationKey}, {deduplicateUntil})
      |on conflict (deduplication_key)
      |  do update
      |  set deduplicate_until={deduplicateUntil}
      |  where pcs.deduplicate_until < {submittedAt}""".stripMargin

  override def upsertDeduplicationEntry(
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

  override def reset(connection: Connection): Unit = {
    SQL("""truncate table configuration_entries cascade;
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
      |""".stripMargin)
      .execute()(connection)
    ()
  }

  override val duplicateKeyError: String = "duplicate key"

  object PostgresQueryStrategy extends QueryStrategy {

    override def arrayIntersectionNonEmptyClause(
        columnName: String,
        parties: Set[Ref.Party],
    ): CompositeSql = {
      import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
      val partiesArray: Array[String] = parties.map(_.toString).toArray
      cSQL"#$columnName::text[] && $partiesArray::text[]"
    }

  }

  override def queryStrategy: QueryStrategy = PostgresQueryStrategy

  object PostgresEventStrategy extends EventStrategy {
    override def filteredEventWitnessesClause(
        witnessesColumnName: String,
        parties: Set[Party],
    ): CompositeSql =
      if (parties.size == 1)
        cSQL"array[${parties.head.toString}]::text[]"
      else {
        val partiesArray: Array[String] = parties.view.map(_.toString).toArray
        cSQL"array(select unnest(#$witnessesColumnName) intersect select unnest($partiesArray::text[]))"
      }

    override def submittersArePartiesClause(
        submittersColumnName: String,
        parties: Set[Party],
    ): CompositeSql = {
      val partiesArray = parties.view.map(_.toString).toArray
      cSQL"(#$submittersColumnName::text[] && $partiesArray::text[])"
    }

    override def witnessesWhereClause(
        witnessesColumnName: String,
        filterParams: FilterParams,
    ): CompositeSql = {
      val wildCardClause = filterParams.wildCardParties match {
        case wildCardParties if wildCardParties.isEmpty =>
          Nil

        case wildCardParties =>
          val partiesArray = wildCardParties.view.map(_.toString).toArray
          cSQL"(#$witnessesColumnName::text[] && $partiesArray::text[])" :: Nil
      }
      val partiesTemplatesClauses =
        filterParams.partiesAndTemplates.iterator.map { case (parties, templateIds) =>
          val partiesArray = parties.view.map(_.toString).toArray
          val templateIdsArray = templateIds.view.map(_.toString).toArray
          cSQL"( (#$witnessesColumnName::text[] && $partiesArray::text[]) AND (template_id = ANY($templateIdsArray::text[])) )"
        }.toList
      (wildCardClause ::: partiesTemplatesClauses).mkComposite("(", " OR ", ")")
    }
  }

  override def eventStrategy: common.EventStrategy = PostgresEventStrategy

  override def maxEventSeqIdForOffset(offset: Offset)(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
    // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
    // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
    SQL"select max(event_sequential_id) from participant_events where event_offset <= $offset group by event_offset order by event_offset desc limit 1"
      .as(get[Long](1).singleOpt)(connection)
  }

  override def createDataSource(
      jdbcUrl: String,
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      connectionInitHook: Option[Connection => Unit],
  )(implicit loggingContext: LoggingContext): DataSource = {
    val pgSimpleDataSource = new PGSimpleDataSource()
    pgSimpleDataSource.setUrl(jdbcUrl)
    val hookFunctions = List(
      dataSourceConfig.postgresConfig.synchronousCommit.toList
        .map(synchCommitValue => exe(s"SET synchronous_commit TO ${synchCommitValue.pgSqlName}")),
      connectionInitHook.toList,
    ).flatten
    InitHookDataSourceProxy(pgSimpleDataSource, hookFunctions)
  }

  override def tryAcquire(
      lockId: DBLockStorageBackend.LockId,
      lockMode: DBLockStorageBackend.LockMode,
  )(connection: Connection): Option[DBLockStorageBackend.Lock] = {
    val lockFunction = lockMode match {
      case DBLockStorageBackend.LockMode.Exclusive => "pg_try_advisory_lock"
      case DBLockStorageBackend.LockMode.Shared => "pg_try_advisory_lock_shared"
    }
    SQL"SELECT #$lockFunction(${pgBigintLockId(lockId)})"
      .as(get[Boolean](1).single)(connection) match {
      case true => Some(DBLockStorageBackend.Lock(lockId, lockMode))
      case false => None
    }
  }

  override def release(lock: DBLockStorageBackend.Lock)(connection: Connection): Boolean = {
    val lockFunction = lock.lockMode match {
      case DBLockStorageBackend.LockMode.Exclusive => "pg_advisory_unlock"
      case DBLockStorageBackend.LockMode.Shared => "pg_advisory_unlock_shared"
    }
    SQL"SELECT #$lockFunction(${pgBigintLockId(lock.lockId)})"
      .as(get[Boolean](1).single)(connection)
  }

  case class PGLockId(id: Long) extends DBLockStorageBackend.LockId

  private def pgBigintLockId(lockId: DBLockStorageBackend.LockId): Long =
    lockId match {
      case PGLockId(id) => id
      case unknown =>
        throw new Exception(
          s"LockId $unknown not supported. Probable cause: LockId was created by a different StorageBackend"
        )
    }

  override def lock(id: Int): DBLockStorageBackend.LockId = PGLockId(id.toLong)

  override def dbLockSupported: Boolean = true
}
