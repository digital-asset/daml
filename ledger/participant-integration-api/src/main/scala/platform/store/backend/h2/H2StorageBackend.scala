// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import java.sql.Connection
import java.time.Instant
import anorm.{Row, SQL, SimpleSql}
import anorm.SqlParser.get
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.appendonlydao.events.ContractId
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
import scala.util.control.NonFatal

private[backend] object H2StorageBackend
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

  private val logger = ContextualizedLogger.get(this.getClass)

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
        |truncate table string_interning;
        |truncate table participant_events_create_filter;
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
          |truncate table string_interning;
          |truncate table participant_events_create_filter;
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
  )(connection: Connection)(implicit loggingContext: LoggingContext): Int = {

    // Under the default READ_COMMITTED isolation level used for the indexdb, when a deduplication
    // upsert is performed simultaneously from multiple threads, the query fails with
    // JdbcSQLIntegrityConstraintViolationException: Unique index or primary key violation
    // Simple retry helps
    def retry[T](op: => T): T =
      try {
        op
      } catch {
        case NonFatal(e) =>
          logger.debug(s"Caught exception while upserting a deduplication entry: $e")
          op
      }
    retry(
      SQL(SQL_INSERT_COMMAND)
        .on(
          "deduplicationKey" -> key,
          "submittedAt" -> Timestamp.instantToMicros(submittedAt),
          "deduplicateUntil" -> Timestamp.instantToMicros(deduplicateUntil),
        )
        .executeUpdate()(connection)
    )
  }

  override def batch(
      dbDtos: Vector[DbDto],
      stringInterning: StringInterning,
  ): AppendOnlySchema.Batch =
    H2Schema.schema.prepareData(dbDtos, stringInterning)

  override def insertBatch(connection: Connection, batch: AppendOnlySchema.Batch): Unit =
    H2Schema.schema.executeUpdate(batch, connection)

  def maxEventSequentialIdOfAnObservableEvent(
      offset: Offset
  )(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL"""
SELECT max_esi FROM (
  (SELECT max(event_sequential_id) AS max_esi FROM participant_events_consuming_exercise WHERE event_offset <= $offset GROUP BY event_offset ORDER BY event_offset DESC FETCH NEXT 1 ROW ONLY)
  UNION ALL
  (SELECT max(event_sequential_id) AS max_esi FROM participant_events_non_consuming_exercise WHERE event_offset <= $offset GROUP BY event_offset ORDER BY event_offset DESC FETCH NEXT 1 ROW ONLY)
  UNION ALL
  (SELECT max(event_sequential_id) AS max_esi FROM participant_events_create WHERE event_offset <= $offset GROUP BY event_offset ORDER BY event_offset DESC FETCH NEXT 1 ROW ONLY)
) AS t
ORDER BY max_esi DESC
FETCH NEXT 1 ROW ONLY;
       """.as(get[Long](1).singleOpt)(connection)
  }

  object H2QueryStrategy extends QueryStrategy {

    override def arrayIntersectionNonEmptyClause(
        columnName: String,
        parties: Set[Ref.Party],
        stringInterning: StringInterning,
    ): CompositeSql = {
      val internedParties = parties.flatMap(stringInterning.party.tryInternalize)
      if (internedParties.isEmpty)
        cSQL"false"
      else
        internedParties.view
          .map(p => cSQL"array_contains(#$columnName, $p)")
          .mkComposite("(", " or ", ")")
    }

    override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
      s"array_contains($arrayColumnName, $elementColumnName)"

    override def isTrue(booleanColumnName: String): String = booleanColumnName
  }

  override def queryStrategy: QueryStrategy = H2QueryStrategy

  object H2EventStrategy extends EventStrategy {
    override def filteredEventWitnessesClause(
        witnessesColumnName: String,
        parties: Set[Ref.Party],
        stringInterning: StringInterning,
    ): CompositeSql = {
      val partiesArray: Array[java.lang.Integer] =
        parties.view.flatMap(stringInterning.party.tryInternalize).map(Int.box).toArray
      if (partiesArray.isEmpty) cSQL"false"
      else cSQL"array_intersection(#$witnessesColumnName, $partiesArray)"
    }

    override def submittersArePartiesClause(
        submittersColumnName: String,
        parties: Set[Ref.Party],
        stringInterning: StringInterning,
    ): CompositeSql =
      H2QueryStrategy.arrayIntersectionNonEmptyClause(
        columnName = submittersColumnName,
        parties = parties,
        stringInterning = stringInterning,
      )

    override def witnessesWhereClause(
        witnessesColumnName: String,
        filterParams: FilterParams,
        stringInterning: StringInterning,
    ): CompositeSql = {
      val wildCardClause = filterParams.wildCardParties match {
        case wildCardParties if wildCardParties.isEmpty =>
          Nil

        case wildCardParties =>
          cSQL"(${H2QueryStrategy.arrayIntersectionNonEmptyClause(witnessesColumnName, wildCardParties, stringInterning)})" :: Nil
      }
      val partiesTemplatesClauses =
        filterParams.partiesAndTemplates.iterator.flatMap { case (parties, templateIds) =>
          val clause =
            H2QueryStrategy.arrayIntersectionNonEmptyClause(
              witnessesColumnName,
              parties,
              stringInterning,
            )
          val templateIdsArray: Array[java.lang.Integer] =
            templateIds.view.flatMap(stringInterning.templateId.tryInternalize).map(Int.box).toArray
          if (templateIdsArray.isEmpty) None
          else Some(cSQL"( ($clause) AND (template_id = ANY($templateIdsArray)) )")
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

  override def maximumLedgerTimeSqlLiteral(
      id: ContractId,
      lastEventSequentialId: Long,
  ): SimpleSql[Row] = {
    import com.daml.platform.store.Conversions.ContractIdToStatement
    SQL"""
  WITH archival_event AS (
         SELECT 1
           FROM participant_events_consuming_exercise
          WHERE contract_id = $id
            AND event_sequential_id <= $lastEventSequentialId
          FETCH NEXT 1 ROW ONLY
       ),
       create_event AS (
         SELECT ledger_effective_time
           FROM participant_events_create
          WHERE contract_id = $id
            AND event_sequential_id <= $lastEventSequentialId
          FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
       ),
       divulged_contract AS (
         SELECT NULL::BIGINT
           FROM participant_events_divulgence
          WHERE contract_id = $id
            AND event_sequential_id <= $lastEventSequentialId
          ORDER BY event_sequential_id
            -- prudent engineering: make results more stable by preferring earlier divulgence events
            -- Results might still change due to pruning.
          FETCH NEXT 1 ROW ONLY
       ),
       create_and_divulged_contracts AS (
         (SELECT * FROM create_event)   -- prefer create over divulgence events
         UNION ALL
         (SELECT * FROM divulged_contract)
       )
  SELECT ledger_effective_time
    FROM create_and_divulged_contracts
   WHERE NOT EXISTS (SELECT 1 FROM archival_event)
   FETCH NEXT 1 ROW ONLY"""
  }

  override def activeContractSqlLiteral(
      contractId: ContractId,
      treeEventWitnessesClause: CompositeSql,
      resultColumns: List[String],
      coalescedColumns: String,
      lastEventSequentialId: Long,
  ): SimpleSql[Row] = {
    import com.daml.platform.store.Conversions.ContractIdToStatement
    SQL"""  WITH archival_event AS (
               SELECT 1
                 FROM participant_events_consuming_exercise
                WHERE contract_id = $contractId
                  AND event_sequential_id <= $lastEventSequentialId
                  AND $treeEventWitnessesClause  -- only use visible archivals
                FETCH NEXT 1 ROW ONLY
             ),
             create_event AS (
               SELECT contract_id, #${resultColumns.mkString(", ")}
                 FROM participant_events_create
                WHERE contract_id = $contractId
                  AND event_sequential_id <= $lastEventSequentialId
                  AND $treeEventWitnessesClause
                FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
             ),
             -- no visibility check, as it is used to backfill missing template_id and create_arguments for divulged contracts
             create_event_unrestricted AS (
               SELECT contract_id, #${resultColumns.mkString(", ")}
                 FROM participant_events_create
                WHERE contract_id = $contractId
                  AND event_sequential_id <= $lastEventSequentialId
                FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
             ),
             divulged_contract AS (
               SELECT divulgence_events.contract_id,
                      -- Note: the divulgence_event.template_id can be NULL
                      -- for certain integrations. For example, the KV integration exploits that
                      -- every participant node knows about all create events. The integration
                      -- therefore only communicates the change in visibility to the IndexDB, but
                      -- does not include a full divulgence event.
                      #$coalescedColumns
                 FROM participant_events_divulgence divulgence_events LEFT OUTER JOIN create_event_unrestricted ON (divulgence_events.contract_id = create_event_unrestricted.contract_id)
                WHERE divulgence_events.contract_id = $contractId -- restrict to aid query planner
                  AND divulgence_events.event_sequential_id <= $lastEventSequentialId
                  AND $treeEventWitnessesClause
                ORDER BY divulgence_events.event_sequential_id
                  -- prudent engineering: make results more stable by preferring earlier divulgence events
                  -- Results might still change due to pruning.
                FETCH NEXT 1 ROW ONLY
             ),
             create_and_divulged_contracts AS (
               (SELECT * FROM create_event)   -- prefer create over divulgence events
               UNION ALL
               (SELECT * FROM divulged_contract)
             )
        SELECT contract_id, #${resultColumns.mkString(", ")}
          FROM create_and_divulged_contracts
         WHERE NOT EXISTS (SELECT 1 FROM archival_event)
         FETCH NEXT 1 ROW ONLY"""
  }

}
