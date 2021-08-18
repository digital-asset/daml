// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection
import java.time.Instant
import anorm.{NamedParameter, SQL, SqlStringInterpolation}
import anorm.SqlParser.get
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.store.appendonlydao.events.{ContractId, Key, Party}
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.backend.common.{
  AppendOnlySchema,
  CommonStorageBackend,
  EventStorageBackendTemplate,
  EventStrategy,
  InitHookDataSourceProxy,
  TemplatedStorageBackend,
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
    with EventStorageBackendTemplate {

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

  override def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(connection: Connection): List[CompletionStreamResponse] =
    TemplatedStorageBackend.commandCompletions(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      applicationId = applicationId,
      submittersInPartiesClause = arrayIntersectionWhereClause("submitters", parties),
    )(connection)

  override def activeContractWithArgument(readers: Set[Ref.Party], contractId: ContractId)(
      connection: Connection
  ): Option[StorageBackend.RawContract] =
    TemplatedStorageBackend.activeContractWithArgument(
      participantTreeWitnessEventsWhereClause = arrayIntersectionWhereClause("tree_event_witnesses", readers),
      divulgenceEventsTreeWitnessWhereClause = arrayIntersectionWhereClause("tree_event_witnesses", readers),
      contractId = contractId,
    )(connection)

  override def activeContractWithoutArgument(readers: Set[Ref.Party], contractId: ContractId)(
      connection: Connection
  ): Option[String] =
    TemplatedStorageBackend.activeContractWithoutArgument(
      treeEventWitnessesWhereClause = arrayIntersectionWhereClause("tree_event_witnesses", readers),
      contractId = contractId,
    )(connection)

  override def contractKey(readers: Set[Ref.Party], key: Key)(
      connection: Connection
  ): Option[ContractId] =
    TemplatedStorageBackend.contractKey(
      flatEventWitnesses = columnPrefix =>
        arrayIntersectionWhereClause(s"$columnPrefix.flat_event_witnesses", readers),
      key = key,
    )(connection)

  object PostgresEventStrategy extends EventStrategy {
    override def filteredEventWitnessesClause(
        witnessesColumnName: String,
        parties: Set[Party],
    ): (String, List[NamedParameter]) =
      if (parties.size == 1)
        (
          s"array[{singlePartyfewc}]::text[]",
          List("singlePartyfewc" -> parties.head.toString),
        )
      else
        (
          s"array(select unnest($witnessesColumnName) intersect select unnest({partiesArrayfewc}::text[]))",
          List("partiesArrayfewc" -> parties.view.map(_.toString).toArray),
        )

    override def submittersArePartiesClause(
        submittersColumnName: String,
        parties: Set[Party],
        columnPrefix: String,
    ): (String, List[NamedParameter]) =
      (
        s"($submittersColumnName::text[] && {wildCardPartiesArraysapc}::text[])",
        List("wildCardPartiesArraysapc" -> parties.view.map(_.toString).toArray),
      )

    override def witnessesWhereClause(
        witnessesColumnName: String,
        filterParams: FilterParams,
        columnPrefix: String,
    ): (String, List[NamedParameter]) = {
      val (wildCardClause, wildCardParams) = filterParams.wildCardParties match {
        case wildCardParties if wildCardParties.isEmpty => (Nil, Nil)
        case wildCardParties =>
          (
            List(s"($witnessesColumnName::text[] && {wildCardPartiesArraywwc}::text[])"),
            List[NamedParameter](
              "wildCardPartiesArraywwc" -> wildCardParties.view.map(_.toString).toArray
            ),
          )
      }
      val (partiesTemplatesClauses, partiesTemplatesParams) =
        filterParams.partiesAndTemplates.iterator.zipWithIndex
          .map { case ((parties, templateIds), index) =>
            (
              s"( ($witnessesColumnName::text[] && {partiesArraywwc$index}::text[]) AND (template_id = ANY({templateIdsArraywwc$index}::text[])) )",
              List[NamedParameter](
                s"partiesArraywwc$index" -> parties.view.map(_.toString).toArray,
                s"templateIdsArraywwc$index" -> templateIds.view.map(_.toString).toArray,
              ),
            )
          }
          .toList
          .unzip
      (
        (wildCardClause ::: partiesTemplatesClauses).mkString("(", " OR ", ")"),
        wildCardParams ::: partiesTemplatesParams.flatten,
      )
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

  // TODO append-only: remove as part of ContractStorageBackend consolidation
  private def format(parties: Set[Party]): String = parties.view.map(p => s"'$p'").mkString(",")

  // TODO append-only: remove as part of ContractStorageBackend consolidation
  private def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Ref.Party]): String =
    s"$arrayColumn::text[] && array[${format(parties)}]::text[]"

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
