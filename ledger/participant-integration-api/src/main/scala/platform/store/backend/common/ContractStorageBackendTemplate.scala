// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.{byteArray, int, long, str}
import anorm.{ResultSetParser, Row, RowParser, SimpleSql, SqlParser, ~}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.store.Conversions.{
  contractId,
  flatEventWitnessesColumn,
  identifier,
  offset,
  timestampFromMicros,
}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.appendonlydao.events.{ContractId, Key}
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.ContractStorageBackend
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}

class ContractStorageBackendTemplate(
                                      queryStrategy: QueryStrategy,
                                      ledgerEndCache: LedgerEndCache,
                                    ) extends ContractStorageBackend {
  override def contractKeyGlobally(key: Key)(connection: Connection): Option[ContractId] =
    contractKey(
      resultColumns = List("contract_id"),
      resultParser = contractId("contract_id"),
    )(
      readers = None,
      key = key,
      validAt = ledgerEndCache()._2,
    )(connection)

  protected def maximumLedgerTimeSqlLiteral(
      id: ContractId,
      lastEventSequentialId: Long,
  ): SimpleSql[Row] = {
    import com.daml.platform.store.Conversions.ContractIdToStatement
    SQL"""
  WITH archival_event AS (
         SELECT participant_events.*
           FROM participant_events
          WHERE contract_id = $id
            AND event_kind = 20  -- consuming exercise
            AND event_sequential_id <= $lastEventSequentialId
          FETCH NEXT 1 ROW ONLY
       ),
       create_event AS (
         SELECT ledger_effective_time
           FROM participant_events
          WHERE contract_id = $id
            AND event_kind = 10  -- create
            AND event_sequential_id <= $lastEventSequentialId
          FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
       ),
       divulged_contract AS (
         SELECT ledger_effective_time
           FROM participant_events
          WHERE contract_id = $id
            AND event_kind = 0 -- divulgence
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

  // TODO append-only: revisit this approach when doing cleanup, so we can decide if it is enough or not.
  // TODO append-only: consider pulling up traversal logic to upper layer
  override def maximumLedgerTime(
      ids: Set[ContractId]
  )(connection: Connection): Either[Set[ContractId], Option[Timestamp]] = {
    val lastEventSequentialId = ledgerEndCache()._2
    def lookup(id: ContractId): Option[Option[Timestamp]] =
        maximumLedgerTimeSqlLiteral(id, lastEventSequentialId).as(
        timestampFromMicros("ledger_effective_time").?.singleOpt
      )(
        connection
      )

    val queriedIds: List[(ContractId, Option[Option[Timestamp]])] = ids.toList
      .map(id => id -> lookup(id))
    val foundLedgerEffectiveTimes: List[Option[Timestamp]] = queriedIds
      .collect { case (_, Some(found)) =>
        found
      }
    if (foundLedgerEffectiveTimes.size != ids.size) {
      val missingIds = queriedIds.collect { case (missingId, None) =>
        missingId
      }
      Left(missingIds.toSet)
    } else Right(foundLedgerEffectiveTimes.max)

  }

  override def keyState(key: Key, validAt: Long)(connection: Connection): KeyState =
    contractKey(
      resultColumns = List("contract_id", "flat_event_witnesses"),
      resultParser = (
        contractId("contract_id")
          ~ flatEventWitnessesColumn("flat_event_witnesses")
      ).map { case cId ~ stakeholders =>
        KeyAssigned(cId, stakeholders)
      },
    )(
      readers = None,
      key = key,
      validAt = validAt,
    )(connection).getOrElse(KeyUnassigned)

  private val fullDetailsContractRowParser: RowParser[ContractStorageBackend.RawContractState] =
    (str("template_id").?
      ~ flatEventWitnessesColumn("flat_event_witnesses")
      ~ byteArray("create_argument").?
      ~ int("create_argument_compression").?
      ~ int("event_kind")
      ~ timestampFromMicros("ledger_effective_time").?)
      .map(SqlParser.flatten)
      .map(ContractStorageBackend.RawContractState.tupled)

  override def contractState(contractId: ContractId, before: Long)(
      connection: Connection
  ): Option[ContractStorageBackend.RawContractState] = {
    import com.daml.platform.store.Conversions.ContractIdToStatement
    SQL"""
           SELECT
             template_id,
             flat_event_witnesses,
             create_argument,
             create_argument_compression,
             event_kind,
             ledger_effective_time
           FROM participant_events
           WHERE
             contract_id = $contractId
             AND event_sequential_id <= $before
             AND (event_kind = 10 OR event_kind = 20)
           ORDER BY event_sequential_id DESC
           FETCH NEXT 1 ROW ONLY"""
      .as(fullDetailsContractRowParser.singleOpt)(connection)
  }

  private val contractStateRowParser: RowParser[ContractStorageBackend.RawContractStateEvent] =
    (int("event_kind") ~
      contractId("contract_id") ~
      identifier("template_id").? ~
      timestampFromMicros("ledger_effective_time").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      byteArray("create_argument").? ~
      int("create_argument_compression").? ~
      long("event_sequential_id") ~
      flatEventWitnessesColumn("flat_event_witnesses") ~
      offset("event_offset")).map {
      case eventKind ~ contractId ~ templateId ~ ledgerEffectiveTime ~ createKeyValue ~ createKeyCompression ~ createArgument ~ createArgumentCompression ~ eventSequentialId ~ flatEventWitnesses ~ offset =>
        ContractStorageBackend.RawContractStateEvent(
          eventKind,
          contractId,
          templateId,
          ledgerEffectiveTime,
          createKeyValue,
          createKeyCompression,
          createArgument,
          createArgumentCompression,
          flatEventWitnesses,
          eventSequentialId,
          offset,
        )
    }

  override def contractStateEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[ContractStorageBackend.RawContractStateEvent] =
    SQL"""
           SELECT
               event_kind,
               contract_id,
               template_id,
               create_key_value,
               create_key_value_compression,
               create_argument,
               create_argument_compression,
               flat_event_witnesses,
               ledger_effective_time,
               event_sequential_id,
               event_offset
           FROM
               participant_events
           WHERE
               event_sequential_id > $startExclusive
               and event_sequential_id <= $endInclusive
               and (event_kind = 10 or event_kind = 20)
           ORDER BY event_sequential_id ASC"""
      .asVectorOf(contractStateRowParser)(connection)

  private val contractRowParser: RowParser[ContractStorageBackend.RawContract] =
    (str("template_id")
      ~ byteArray("create_argument")
      ~ int("create_argument_compression").?)
      .map(SqlParser.flatten)
      .map { case (templateId, createArgument, createArgumentCompression) =>
        new ContractStorageBackend.RawContract(
          templateId,
          createArgument,
          createArgumentCompression,
        )
      }

  protected def activeContractSqlLiteral(
      contractId: ContractId,
      treeEventWitnessesClause: CompositeSql,
      resultColumns: List[String],
      coalescedColumns: String,
  ): SimpleSql[Row] = {
    val lastEventSequentialId = ledgerEndCache()._2
    import com.daml.platform.store.Conversions.ContractIdToStatement
    SQL"""  WITH archival_event AS (
               SELECT participant_events.*
                 FROM participant_events
                WHERE contract_id = $contractId
                  AND event_kind = 20  -- consuming exercise
                  AND event_sequential_id <= $lastEventSequentialId
                  AND $treeEventWitnessesClause  -- only use visible archivals
                FETCH NEXT 1 ROW ONLY
             ),
             create_event AS (
               SELECT contract_id, #${resultColumns.mkString(", ")}
                 FROM participant_events
                WHERE contract_id = $contractId
                  AND event_kind = 10  -- create
                  AND event_sequential_id <= $lastEventSequentialId
                  AND $treeEventWitnessesClause
                FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
             ),
             -- no visibility check, as it is used to backfill missing template_id and create_arguments for divulged contracts
             create_event_unrestricted AS (
               SELECT contract_id, #${resultColumns.mkString(", ")}
                 FROM participant_events
                WHERE contract_id = $contractId
                  AND event_kind = 10  -- create
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
                 FROM participant_events divulgence_events LEFT OUTER JOIN create_event_unrestricted ON (divulgence_events.contract_id = create_event_unrestricted.contract_id)
                WHERE divulgence_events.contract_id = $contractId -- restrict to aid query planner
                  AND divulgence_events.event_kind = 0 -- divulgence
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

  private def activeContract[T](
      resultSetParser: ResultSetParser[T],
      resultColumns: List[String],
  )(
      readers: Set[Ref.Party],
      contractId: ContractId,
  )(connection: Connection): T = {
    val treeEventWitnessesClause: CompositeSql =
      queryStrategy.arrayIntersectionNonEmptyClause(
        columnName = "tree_event_witnesses",
        parties = readers,
      )
    val coalescedColumns: String = resultColumns
      .map(columnName =>
        s"COALESCE(divulgence_events.$columnName, create_event_unrestricted.$columnName)"
      )
      .mkString(", ")
    activeContractSqlLiteral(contractId, treeEventWitnessesClause, resultColumns, coalescedColumns)
      .as(resultSetParser)(connection)
  }

  private val contractWithoutValueRowParser: RowParser[String] =
    str("template_id")

  override def activeContractWithArgument(
      readers: Set[Ref.Party],
      contractId: ContractId,
  )(connection: Connection): Option[ContractStorageBackend.RawContract] = {
    activeContract(
      resultSetParser = contractRowParser.singleOpt,
      resultColumns = List("template_id", "create_argument", "create_argument_compression"),
    )(
      readers = readers,
      contractId = contractId,
    )(connection)
  }

  override def activeContractWithoutArgument(
      readers: Set[Ref.Party],
      contractId: ContractId,
  )(connection: Connection): Option[String] =
    activeContract(
      resultSetParser = contractWithoutValueRowParser.singleOpt,
      resultColumns = List("template_id"),
    )(
      readers = readers,
      contractId = contractId,
    )(connection)

  override def contractKey(readers: Set[Ref.Party], key: Key)(
      connection: Connection
  ): Option[ContractId] =
    contractKey(
      resultColumns = List("contract_id"),
      resultParser = contractId("contract_id"),
    )(
      readers = Some(readers),
      key = key,
      validAt = ledgerEndCache()._2,
    )(connection)

  private def contractKey[T](
      resultColumns: List[String],
      resultParser: RowParser[T],
  )(
      readers: Option[Set[Ref.Party]],
      key: Key,
      validAt: Long,
  )(
      connection: Connection
  ): Option[T] = {
    def withAndIfNonEmptyReaders(
        queryF: Set[Ref.Party] => CompositeSql
    ): CompositeSql =
      readers match {
        case Some(readers) =>
          cSQL"${queryF(readers)} AND"

        case None =>
          cSQL""
      }

    val lastContractKeyFlatEventWitnessesClause =
      withAndIfNonEmptyReaders(
        queryStrategy.arrayIntersectionNonEmptyClause(
          columnName = "last_contract_key_create.flat_event_witnesses",
          _,
        )
      )
    val participantEventsFlatEventWitnessesClause =
      withAndIfNonEmptyReaders(
        queryStrategy.arrayIntersectionNonEmptyClause(
          columnName = "participant_events.flat_event_witnesses",
          _,
        )
      )

    import com.daml.platform.store.Conversions.HashToStatement
    SQL"""
         WITH last_contract_key_create AS (
                SELECT participant_events.*
                  FROM participant_events
                 WHERE event_kind = 10 -- create
                   AND create_key_hash = ${key.hash}
                       -- do NOT check visibility here, as otherwise we do not abort the scan early
                   AND event_sequential_id <= $validAt
                 ORDER BY event_sequential_id DESC
                 FETCH NEXT 1 ROW ONLY
              )
         SELECT #${resultColumns.mkString(", ")}
           FROM last_contract_key_create -- creation only, as divulged contracts cannot be fetched by key
         WHERE $lastContractKeyFlatEventWitnessesClause -- check visibility only here
           NOT EXISTS       -- check no archival visible
                (SELECT 1
                   FROM participant_events
                  WHERE event_kind = 20 AND -- consuming exercise
                    $participantEventsFlatEventWitnessesClause
                    contract_id = last_contract_key_create.contract_id
                    AND event_sequential_id <= $validAt
                )"""
      .as(resultParser.singleOpt)(connection)
  }
}
