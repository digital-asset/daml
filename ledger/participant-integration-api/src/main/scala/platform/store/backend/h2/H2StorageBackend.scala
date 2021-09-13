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
  Timestamp,
}
import com.daml.platform.store.backend.{
  DBLockStorageBackend,
  DataSourceStorageBackend,
  DbDto,
  StorageBackend,
  common,
}
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

  override def upsertDeduplicationEntry(
      key: String,
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(connection: Connection): Int =
    SQL(SQL_INSERT_COMMAND)
      .on(
        "deduplicationKey" -> key,
        "submittedAt" -> Timestamp.instantToMicros(submittedAt),
        "deduplicateUntil" -> Timestamp.instantToMicros(deduplicateUntil),
      )
      .executeUpdate()(connection)

  override def batch(dbDtos: Vector[DbDto]): AppendOnlySchema.Batch =
    H2Schema.schema.prepareData(dbDtos)

  override def insertBatch(connection: Connection, batch: AppendOnlySchema.Batch): Unit =
    H2Schema.schema.executeUpdate(batch, connection)

  def maxEventSequentialIdOfAnObservableEvent(
      offset: Offset
  )(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
    // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
    // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
    SQL"""
select max_esi from (
  (SELECT max(event_sequential_id) as max_esi FROM participant_events_consuming_exercise WHERE event_offset <= $offset GROUP BY event_offset ORDER BY event_offset DESC FETCH NEXT 1 ROW only)
  union all
  (SELECT max(event_sequential_id) as max_esi FROM participant_events_non_consuming_exercise WHERE event_offset <= $offset GROUP BY event_offset ORDER BY event_offset DESC FETCH NEXT 1 ROW only)
  union all
  (SELECT max(event_sequential_id) as max_esi FROM participant_events_create WHERE event_offset <= $offset GROUP BY event_offset ORDER BY event_offset DESC FETCH NEXT 1 ROW only)
) as t
order by max_esi desc
fetch next 1 row only;
       """.as(get[Long](1).singleOpt)(connection)
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

    override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
      s"array_contains($arrayColumnName, $elementColumnName)"

    override def isTrue(booleanColumnName: String): String = booleanColumnName
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
    val (urlNoUserNoPassword, user, password) = extractUserPasswordAndRemoveFromUrl(jdbcUrl)
    user.foreach(h2DataSource.setUser)
    password.foreach(h2DataSource.setPassword)
    h2DataSource.setUrl(urlNoUserNoPassword)

    InitHookDataSourceProxy(h2DataSource, connectionInitHook.toList)
  }

  def extractUserPasswordAndRemoveFromUrl(
      jdbcUrl: String
  ): (String, Option[String], Option[String]) = {
    def setKeyValueAndRemoveFromUrl(url: String, key: String): (String, Option[String]) = {
      val regex = s".*(;(?i)${key}=([^;]*)).*".r
      url match {
        case regex(keyAndValue, value) =>
          (url.replace(keyAndValue, ""), Some(value))
        case _ => (url, None)
      }
    }

    val (urlNoUser, user) = setKeyValueAndRemoveFromUrl(jdbcUrl, "user")
    val (urlNoUserNoPassword, password) = setKeyValueAndRemoveFromUrl(urlNoUser, "password")
    (urlNoUserNoPassword, user, password)
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
  ): Unit = ()
}
