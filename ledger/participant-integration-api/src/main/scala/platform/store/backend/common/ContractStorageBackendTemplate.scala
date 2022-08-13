// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import anorm.SqlParser.{array, byteArray, int, long}
import anorm.{ResultSetParser, Row, RowParser, SimpleSql, SqlParser, ~}
import com.daml.platform.{ContractId, Key, Party}
import com.daml.platform.store.backend.Conversions.{contractId, offset, timestampFromMicros}
import com.daml.ledger.offset.Offset
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.ContractStorageBackend
import com.daml.platform.store.backend.ContractStorageBackend.RawContractState
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}
import com.daml.platform.store.interning.StringInterning

class ContractStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
) extends ContractStorageBackend {
  import com.daml.platform.store.backend.Conversions.ArrayColumnToIntArray._

  override def keyState(key: Key, validAt: Offset)(connection: Connection): KeyState = {
    val resultParser =
      (contractId("contract_id") ~ array[Int]("flat_event_witnesses")).map {
        case cId ~ stakeholders =>
          KeyAssigned(cId, stakeholders.view.map(stringInterning.party.externalize).toSet)
      }.singleOpt

    import com.daml.platform.store.backend.Conversions.HashToStatement
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
         WITH last_contract_key_create AS (
                SELECT participant_events_create.*
                  FROM participant_events_create
                 WHERE create_key_hash = ${key.hash}
                   AND event_offset <= $validAt
                 ORDER BY event_sequential_id DESC
                 FETCH NEXT 1 ROW ONLY
              )
         SELECT contract_id, flat_event_witnesses
           FROM last_contract_key_create -- creation only, as divulged contracts cannot be fetched by key
         WHERE NOT EXISTS
                (SELECT 1
                   FROM participant_events_consuming_exercise
                  WHERE
                    contract_id = last_contract_key_create.contract_id
                    AND event_offset <= $validAt
                )"""
      .as(resultParser)(connection)
      .getOrElse(KeyUnassigned)
  }

  private val fullDetailsContractRowParser: RowParser[ContractStorageBackend.RawContractState] =
    (int("template_id").?
      ~ array[Int]("flat_event_witnesses")
      ~ byteArray("create_argument").?
      ~ int("create_argument_compression").?
      ~ int("event_kind")
      ~ timestampFromMicros("ledger_effective_time").?)
      .map {
        case internalTemplateId ~ flatEventWitnesses ~ createArgument ~ createArgumentCompression ~ eventKind ~ ledgerEffectiveTime =>
          RawContractState(
            templateId = internalTemplateId.map(stringInterning.templateId.unsafe.externalize),
            flatEventWitnesses = flatEventWitnesses.view
              .map(stringInterning.party.externalize)
              .toSet,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            eventKind = eventKind,
            ledgerEffectiveTime = ledgerEffectiveTime,
          )
      }

  override def contractState(contractId: ContractId, before: Offset)(
      connection: Connection
  ): Option[ContractStorageBackend.RawContractState] = {
    import com.daml.platform.store.backend.Conversions.ContractIdToStatement
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
           (SELECT
             event_sequential_id,
             template_id,
             flat_event_witnesses,
             create_argument,
             create_argument_compression,
             10 as event_kind,
             ledger_effective_time
           FROM participant_events_create
           WHERE
             contract_id = $contractId
             AND event_offset <= $before)
           UNION ALL
           (SELECT
             event_sequential_id,
             template_id,
             flat_event_witnesses,
             NULL as create_argument,
             NULL as create_argument_compression,
             20 as event_kind,
             ledger_effective_time
           FROM participant_events_consuming_exercise
           WHERE
             contract_id = $contractId
             AND event_offset <= $before)
           ORDER BY event_sequential_id DESC
           FETCH NEXT 1 ROW ONLY"""
      .as(fullDetailsContractRowParser.singleOpt)(connection)
  }

  private val contractStateRowParser: RowParser[ContractStorageBackend.RawContractStateEvent] =
    (int("event_kind") ~
      contractId("contract_id") ~
      int("template_id").? ~
      timestampFromMicros("ledger_effective_time").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      byteArray("create_argument").? ~
      int("create_argument_compression").? ~
      long("event_sequential_id") ~
      array[Int]("flat_event_witnesses") ~
      offset("event_offset")).map {
      case eventKind ~ contractId ~ internalTemplateId ~ ledgerEffectiveTime ~ createKeyValue ~ createKeyCompression ~ createArgument ~ createArgumentCompression ~ eventSequentialId ~ flatEventWitnesses ~ offset =>
        ContractStorageBackend.RawContractStateEvent(
          eventKind,
          contractId,
          internalTemplateId.map(stringInterning.templateId.externalize),
          ledgerEffectiveTime,
          createKeyValue,
          createKeyCompression,
          createArgument,
          createArgumentCompression,
          flatEventWitnesses.view
            .map(stringInterning.party.externalize)
            .toSet,
          eventSequentialId,
          offset,
        )
    }

  override def contractStateEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[ContractStorageBackend.RawContractStateEvent] = {
    SQL"""
         (SELECT
               10 as event_kind,
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
               participant_events_create
           WHERE
               event_sequential_id > $startExclusive
               and event_sequential_id <= $endInclusive)
         UNION ALL
         (SELECT
               20 as event_kind,
               contract_id,
               template_id,
               create_key_value,
               create_key_value_compression,
               NULL as create_argument,
               NULL as create_argument_compression,
               flat_event_witnesses,
               ledger_effective_time,
               event_sequential_id,
               event_offset
           FROM
               participant_events_consuming_exercise
           WHERE
               event_sequential_id > $startExclusive
               and event_sequential_id <= $endInclusive)
         ORDER BY event_sequential_id ASC"""
      .asVectorOf(contractStateRowParser)(connection)
  }

  private val contractRowParser: RowParser[ContractStorageBackend.RawContract] =
    (int("template_id")
      ~ byteArray("create_argument")
      ~ int("create_argument_compression").?)
      .map(SqlParser.flatten)
      .map { case (internalTemplateId, createArgument, createArgumentCompression) =>
        new ContractStorageBackend.RawContract(
          stringInterning.templateId.unsafe.externalize(internalTemplateId),
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
    import com.daml.platform.store.backend.Conversions.ContractIdToStatement
    SQL"""  WITH archival_event AS (
               SELECT participant_events_consuming_exercise.*
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

  private def activeContract[T](
      resultSetParser: ResultSetParser[Option[T]],
      resultColumns: List[String],
  )(
      readers: Set[Party],
      contractId: ContractId,
  )(connection: Connection): Option[T] = {
    val internedReaders =
      readers.view.map(stringInterning.party.tryInternalize).flatMap(_.toList).toSet
    if (internedReaders.isEmpty) {
      None
    } else {
      val treeEventWitnessesClause: CompositeSql =
        queryStrategy.arrayIntersectionNonEmptyClause(
          columnName = "tree_event_witnesses",
          internedParties = internedReaders,
        )
      val coalescedColumns: String = resultColumns
        .map(columnName =>
          s"COALESCE(divulgence_events.$columnName, create_event_unrestricted.$columnName) as $columnName"
        )
        .mkString(", ")
      activeContractSqlLiteral(
        contractId,
        treeEventWitnessesClause,
        resultColumns,
        coalescedColumns,
      )
        .as(resultSetParser)(connection)
    }
  }

  private val contractWithoutValueRowParser: RowParser[Int] =
    int("template_id")

  override def activeContractWithArgument(
      readers: Set[Party],
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
      readers: Set[Party],
      contractId: ContractId,
  )(connection: Connection): Option[String] =
    activeContract(
      resultSetParser = contractWithoutValueRowParser.singleOpt,
      resultColumns = List("template_id"),
    )(
      readers = readers,
      contractId = contractId,
    )(connection).map(stringInterning.templateId.unsafe.externalize)
}
