// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import java.sql.Connection
import java.time.Instant

import anorm.SQL
import anorm.SqlParser.get
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
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
  PartyStorageBackendTemplate,
  QueryStrategy,
}
import com.daml.platform.store.backend.{
  DBLockStorageBackend,
  DataSourceStorageBackend,
  DbDto,
  StorageBackend,
  common,
}
import com.daml.scalautil.Statement.discard
import javax.sql.DataSource

private[backend] object H2StorageBackend
    extends StorageBackend[AppendOnlySchema.Batch]
    with CommonStorageBackend[AppendOnlySchema.Batch]
    with EventStorageBackendTemplate
    with ContractStorageBackendTemplate
    with CompletionStorageBackendTemplate
    with PartyStorageBackendTemplate {

  override def reset(connection: Connection): Unit = {
    SQL("""set referential_integrity false;
        |truncate table configuration_entries;
        |truncate table package_entries;
        |truncate table parameters;
        |truncate table participant_command_completions;
        |truncate table participant_command_submissions;
        |truncate table participant_events_divulgence;
        |truncate table participant_events_create;
        |truncate table participant_events_consuming_exercise;
        |truncate table participant_events_non_consuming_exercise;
        |truncate table party_entries;
        |set referential_integrity true;""".stripMargin)
      .execute()(connection)
    ()
  }

  override def resetAll(connection: Connection): Unit = {
    SQL("""set referential_integrity false;
          |truncate table configuration_entries;
          |truncate table packages;
          |truncate table package_entries;
          |truncate table parameters;
          |truncate table participant_command_completions;
          |truncate table participant_command_submissions;
          |truncate table participant_events_divulgence;
          |truncate table participant_events_create;
          |truncate table participant_events_consuming_exercise;
          |truncate table participant_events_non_consuming_exercise;
          |truncate table party_entries;
          |set referential_integrity true;""".stripMargin)
      .execute()(connection)
    ()
  }

  val SQL_INSERT_COMMAND: String =
    """merge into participant_command_submissions pcs
      |using dual on deduplication_key = {deduplicationKey}
      |when not matched then
      |  insert (deduplication_key, deduplicate_until)
      |  values ({deduplicationKey}, {deduplicateUntil})
      |when matched and pcs.deduplicate_until < {submittedAt} then
      |  update set deduplicate_until={deduplicateUntil}""".stripMargin

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
    H2Schema.schema.prepareData(dbDtos)

  override def insertBatch(connection: Connection, batch: AppendOnlySchema.Batch): Unit =
    H2Schema.schema.executeUpdate(batch, connection)

  // TODO FIXME: this is for postgres not for H2
  def maxEventSeqIdForOffset(offset: Offset)(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
    // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
    // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
    SQL"select max(event_sequential_id) from participant_events where event_offset <= $offset group by event_offset order by event_offset desc limit 1"
      .as(get[Long](1).singleOpt)(connection)
  }

  object H2QueryStrategy extends QueryStrategy {

    override def arrayIntersectionNonEmptyClause(
        columnName: String,
        parties: Set[Ref.Party],
    ): CompositeSql =
      if (parties.isEmpty)
        cSQL"false"
      else
        parties.view
          .map(p => cSQL"array_contains(#$columnName, '#${p.toString}')")
          .mkComposite("(", " or ", ")")

  }

  override def queryStrategy: QueryStrategy = H2QueryStrategy

  object H2EventStrategy extends EventStrategy {
    override def filteredEventWitnessesClause(
        witnessesColumnName: String,
        parties: Set[Ref.Party],
    ): CompositeSql = {
      val partiesArray = parties.view.map(_.toString).toArray
      cSQL"array_intersection(#$witnessesColumnName, $partiesArray)"
    }

    override def submittersArePartiesClause(
        submittersColumnName: String,
        parties: Set[Ref.Party],
    ): CompositeSql =
      H2QueryStrategy.arrayIntersectionNonEmptyClause(
        columnName = submittersColumnName,
        parties = parties,
      )

    override def witnessesWhereClause(
        witnessesColumnName: String,
        filterParams: FilterParams,
    ): CompositeSql = {
      val wildCardClause = filterParams.wildCardParties match {
        case wildCardParties if wildCardParties.isEmpty =>
          Nil

        case wildCardParties =>
          cSQL"(${H2QueryStrategy.arrayIntersectionNonEmptyClause(witnessesColumnName, wildCardParties)})" :: Nil
      }
      val partiesTemplatesClauses =
        filterParams.partiesAndTemplates.iterator.map { case (parties, templateIds) =>
          val clause =
            H2QueryStrategy.arrayIntersectionNonEmptyClause(
              witnessesColumnName,
              parties,
            )
          val templateIdsArray = templateIds.view.map(_.toString).toArray
          cSQL"( ($clause) AND (template_id = ANY($templateIdsArray)) )"
        }.toList
      (wildCardClause ::: partiesTemplatesClauses).mkComposite("(", " OR ", ")")
    }
  }

  override def eventStrategy: common.EventStrategy = H2EventStrategy

  override def createDataSource(
      jdbcUrl: String,
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      connectionInitHook: Option[Connection => Unit],
  )(implicit loggingContext: LoggingContext): DataSource = {
    val h2DataSource = new org.h2.jdbcx.JdbcDataSource()

    // H2 (org.h2.jdbcx.JdbcDataSource) does not support setting the user/password within the jdbcUrl, so remove
    // those properties from the url if present and set them separately. Note that Postgres and Oracle support
    // user/password in the URLs, so we don't bother exposing user/password configs separately from the url just for h2
    // which is anyway not supported for production. (This also helps run canton h2 participants that set user and
    // password.)
    val urlNoUser = setKeyValueAndRemoveFromUrl(jdbcUrl, "user", h2DataSource.setUser)
    val urlNoUserNoPassword =
      setKeyValueAndRemoveFromUrl(urlNoUser, "password", h2DataSource.setPassword)
    h2DataSource.setUrl(urlNoUserNoPassword)

    InitHookDataSourceProxy(h2DataSource, connectionInitHook.toList)
  }

  def setKeyValueAndRemoveFromUrl(url: String, key: String, setter: String => Unit): String = {
    val separator = ";"
    url.toLowerCase.indexOf(separator + key + "=") match {
      case -1 => url // leave url intact if key is not found
      case indexKeyValueBegin =>
        val valueBegin = indexKeyValueBegin + 1 + key.length + 1 // separator, key, "="
        val (value, shortenedUrl) = url.indexOf(separator, indexKeyValueBegin + 1) match {
          case -1 => (url.substring(valueBegin), url.take(indexKeyValueBegin))
          case indexKeyValueEnd =>
            (
              url.substring(valueBegin, indexKeyValueEnd),
              url.take(indexKeyValueBegin) + url.takeRight(url.length - indexKeyValueEnd),
            )
        }
        setter(value)
        shortenedUrl
    }
  }

  override def tryAcquire(
      lockId: DBLockStorageBackend.LockId,
      lockMode: DBLockStorageBackend.LockMode,
  )(connection: Connection): Option[DBLockStorageBackend.Lock] =
    throw new UnsupportedOperationException("db level locks are not supported for H2")

  override def release(lock: DBLockStorageBackend.Lock)(connection: Connection): Boolean =
    throw new UnsupportedOperationException("db level locks are not supported for H2")

  override def lock(id: Int): DBLockStorageBackend.LockId =
    throw new UnsupportedOperationException("db level locks are not supported for H2")

  override def dbLockSupported: Boolean = false

  // Migration from mutable schema is not supported for H2
  override def validatePruningOffsetAgainstMigration(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      connection: Connection,
  ): Unit = {
    discard(pruneUpToInclusive)
    discard(pruneAllDivulgedContracts)
    discard(connection)
  }
}
