// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection
import java.time.Instant
import anorm.SQL
import anorm.SqlParser.{get, int}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.events.Party
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.{
  AppendOnlySchema,
  CompletionStorageBackendTemplate,
  ConfigurationStorageBackendTemplate,
  ContractStorageBackendTemplate,
  DataSourceStorageBackendTemplate,
  IntegrityStorageBackendTemplate,
  DeduplicationStorageBackendTemplate,
  EventStorageBackendTemplate,
  EventStrategy,
  IngestionStorageBackendTemplate,
  InitHookDataSourceProxy,
  PackageStorageBackendTemplate,
  ParameterStorageBackendTemplate,
  PartyStorageBackendTemplate,
  QueryStrategy,
  StringInterningStorageBackendTemplate,
  Timestamp,
}
import com.daml.platform.store.backend.{
  DBLockStorageBackend,
  DataSourceStorageBackend,
  DbDto,
  StorageBackend,
  common,
}
import com.daml.platform.store.cache.StringInterning

import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource

private[backend] object PostgresStorageBackend
    extends StorageBackend[AppendOnlySchema.Batch]
    with DataSourceStorageBackendTemplate
    with IngestionStorageBackendTemplate[AppendOnlySchema.Batch]
    with ParameterStorageBackendTemplate
    with ConfigurationStorageBackendTemplate
    with PackageStorageBackendTemplate
    with DeduplicationStorageBackendTemplate
    with EventStorageBackendTemplate
    with ContractStorageBackendTemplate
    with CompletionStorageBackendTemplate
    with PartyStorageBackendTemplate
    with IntegrityStorageBackendTemplate
    with StringInterningStorageBackendTemplate {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def insertBatch(
      connection: Connection,
      postgresDbBatch: AppendOnlySchema.Batch,
  ): Unit =
    PGSchema.schema.executeUpdate(postgresDbBatch, connection)

  override def batch(
      dbDtos: Vector[DbDto],
      stringInterning: StringInterning,
  ): AppendOnlySchema.Batch =
    PGSchema.schema.prepareData(dbDtos, stringInterning)

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
  )(connection: Connection)(implicit loggingContext: LoggingContext): Int =
    SQL(SQL_INSERT_COMMAND)
      .on(
        "deduplicationKey" -> key,
        "submittedAt" -> Timestamp.instantToMicros(submittedAt),
        "deduplicateUntil" -> Timestamp.instantToMicros(deduplicateUntil),
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
      |truncate table party_entries cascade;
      |truncate table string_interning;
      |""".stripMargin)
      .execute()(connection)
    ()
  }

  override def resetAll(connection: Connection): Unit = {
    SQL("""truncate table configuration_entries cascade;
          |truncate table packages cascade;
          |truncate table package_entries cascade;
          |truncate table parameters cascade;
          |truncate table participant_command_completions cascade;
          |truncate table participant_command_submissions cascade;
          |truncate table participant_events_divulgence cascade;
          |truncate table participant_events_create cascade;
          |truncate table participant_events_consuming_exercise cascade;
          |truncate table participant_events_non_consuming_exercise cascade;
          |truncate table party_entries cascade;
          |truncate table string_interning cascade;
          |""".stripMargin)
      .execute()(connection)
    ()
  }

  /** If `pruneAllDivulgedContracts` is set, validate that the pruning offset is after
    * the last ingested event offset (if exists) before the migration to append-only schema
    * (see [[com.daml.platform.store.appendonlydao.JdbcLedgerDao.prune]])
    */
  def validatePruningOffsetAgainstMigration(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      connection: Connection,
  ): Unit =
    if (pruneAllDivulgedContracts) {
      import com.daml.platform.store.Conversions.OffsetToStatement
      SQL"""
       with max_offset_before_migration as (
         select max(event_offset) as max_event_offset
         from participant_events, participant_migration_history_v100
         where event_sequential_id <= ledger_end_sequential_id_before
       )
       select 1 as result
       from max_offset_before_migration
       where max_event_offset >= $pruneUpToInclusive
       """
        .as(int("result").singleOpt)(connection)
        .foreach(_ =>
          throw ErrorFactories.invalidArgument(None)(
            "Pruning offset for all divulged contracts needs to be after the migration offset"
          )
        )
    }

  object PostgresQueryStrategy extends QueryStrategy {

    override def arrayIntersectionNonEmptyClause(
        columnName: String,
        parties: Set[Ref.Party],
        stringInterning: StringInterning,
    ): CompositeSql = {
      import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
      val partiesArray: Array[java.lang.Integer] =
        parties.flatMap(party => stringInterning.party.tryInternalize(party).map(Int.box)).toArray
      cSQL"#$columnName::int[] && $partiesArray::int[]"
    }

    override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
      s"$elementColumnName = any($arrayColumnName)"

    override def isTrue(booleanColumnName: String): String = booleanColumnName
  }

  override def queryStrategy: QueryStrategy = PostgresQueryStrategy

  object PostgresEventStrategy extends EventStrategy {
    override def filteredEventWitnessesClause(
        witnessesColumnName: String,
        parties: Set[Party],
        stringInterning: StringInterning,
    ): CompositeSql = {
      val internedParties: Array[java.lang.Integer] = parties.view
        .flatMap(party => stringInterning.party.tryInternalize(party).map(Int.box))
        .toArray
      if (internedParties.length == 1)
        cSQL"array[${internedParties.head}]::integer[]"
      else
        cSQL"array(select unnest(#$witnessesColumnName) intersect select unnest($internedParties::integer[]))" // TODO why are we doing this in SQL??
    }

    override def submittersArePartiesClause(
        submittersColumnName: String,
        parties: Set[Party],
        stringInterning: StringInterning,
    ): CompositeSql = {
      val partiesArray: Array[java.lang.Integer] = parties.view
        .flatMap(party => stringInterning.party.tryInternalize(party).map(Int.box))
        .toArray
      cSQL"(#$submittersColumnName::integer[] && $partiesArray::integer[])"
    }

    override def witnessesWhereClause(
        witnessesColumnName: String,
        filterParams: FilterParams,
        stringInterning: StringInterning,
    ): CompositeSql = {
      val wildCardClause = filterParams.wildCardParties match {
        case wildCardParties if wildCardParties.isEmpty =>
          Nil

        case wildCardParties =>
          val partiesArray: Array[java.lang.Integer] = wildCardParties.view
            .flatMap(party => stringInterning.party.tryInternalize(party).map(Int.box))
            .toArray
          if (partiesArray.isEmpty)
            Nil
          else
            cSQL"(#$witnessesColumnName::integer[] && $partiesArray::integer[])" :: Nil
      }
      val partiesTemplatesClauses =
        filterParams.partiesAndTemplates.iterator
          .map { case (parties, templateIds) =>
            (
              parties.flatMap(s => stringInterning.party.tryInternalize(s)),
              templateIds.flatMap(s => stringInterning.templateId.tryInternalize(s)),
            )
          }
          .filterNot(_._1.isEmpty)
          .filterNot(_._2.isEmpty)
          .map { case (parties, templateIds) =>
            val partiesArray: Array[java.lang.Integer] = parties.view.map(Int.box).toArray
            val templateIdsArray: Array[java.lang.Integer] =
              templateIds.view
                .map(Int.box)
                .toArray // TODO anorm does not like primitive arrays it seem, so we need to box it
            cSQL"( (#$witnessesColumnName::integer[] && $partiesArray::integer[]) AND (template_id = ANY($templateIdsArray::integer[])) )"
          }
          .toList

      wildCardClause ::: partiesTemplatesClauses match {
        case Nil => cSQL"false"
        case allClauses => allClauses.mkComposite("(", " OR ", ")")
      }
    }
  }

  override def eventStrategy: common.EventStrategy = PostgresEventStrategy

  // TODO FIXME: Use tables directly instead of the participant_events view.
  override def maxEventSequentialIdOfAnObservableEvent(
      offset: Offset
  )(connection: Connection): Option[Long] = {
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

  override def checkCompatibility(
      connection: Connection
  )(implicit loggingContext: LoggingContext): Unit = {
    getPostgresVersion(connection) match {
      case Some((major, minor)) =>
        if (major < 10) {
          logger.error(
            "Deprecated Postgres version. " +
              s"Found Postgres version $major.$minor, minimum required Postgres version is 10. " +
              "This application will continue running but is at risk of data loss, as Postgres < 10 does not support crash-fault tolerant hash indices. " +
              "Please upgrade your Postgres database to version 10 or later to fix this issue."
          )
        }
      case None =>
        logger.warn(
          s"Could not determine the version of the Postgres database. Please verify that this application is compatible with this Postgres version."
        )
    }
    ()
  }

  private[backend] def getPostgresVersion(
      connection: Connection
  )(implicit loggingContext: LoggingContext): Option[(Int, Int)] = {
    val version = SQL"SHOW server_version".as(get[String](1).single)(connection)
    logger.debug(s"Found Postgres version $version")
    parsePostgresVersion(version)
  }

  private[backend] def parsePostgresVersion(version: String): Option[(Int, Int)] = {
    val versionPattern = """(\d+)[.](\d+).*""".r
    version match {
      case versionPattern(major, minor) => Some((major.toInt, minor.toInt))
      case _ => None
    }
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
