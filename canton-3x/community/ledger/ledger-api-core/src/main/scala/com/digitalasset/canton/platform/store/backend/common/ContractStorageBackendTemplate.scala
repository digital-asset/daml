// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{array, byteArray, int, str}
import anorm.{ResultSetParser, Row, RowParser, SimpleSql, SqlParser, ~}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.{
  RawArchivedContract,
  RawCreatedContract,
}
import com.digitalasset.canton.platform.store.backend.Conversions.{contractId, timestampFromMicros}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{ContractId, Key, Party}

import java.sql.Connection

class ContractStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
) extends ContractStorageBackend {
  import com.digitalasset.canton.platform.store.backend.Conversions.ArrayColumnToIntArray.*

  override def keyState(key: Key, validAt: Offset)(connection: Connection): KeyState = {
    val resultParser =
      (contractId("contract_id") ~ array[Int]("flat_event_witnesses")).map {
        case cId ~ stakeholders =>
          KeyAssigned(cId, stakeholders.view.map(stringInterning.party.externalize).toSet)
      }.singleOpt

    import com.digitalasset.canton.platform.store.backend.Conversions.HashToStatement
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
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

  private val archivedContractRowParser: RowParser[(ContractId, RawArchivedContract)] =
    (str("contract_id") ~ array[Int]("flat_event_witnesses"))
      .map { case coid ~ flatEventWitnesses =>
        ContractId.assertFromString(coid) -> RawArchivedContract(
          flatEventWitnesses = flatEventWitnesses.view
            .map(stringInterning.party.externalize)
            .toSet
        )
      }

  override def archivedContracts(contractIds: Seq[ContractId], before: Offset)(
      connection: Connection
  ): Map[ContractId, RawArchivedContract] =
    if (contractIds.isEmpty) Map.empty
    else {
      import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
      SQL"""
       SELECT contract_id, flat_event_witnesses
       FROM participant_events_consuming_exercise
       WHERE
         contract_id ${queryStrategy.anyOfStrings(contractIds.map(_.coid))}
         AND event_offset <= $before"""
        .as(archivedContractRowParser.*)(connection)
        .toMap
    }

  private val rawCreatedContractRowParser
      : RowParser[(ContractId, ContractStorageBackend.RawCreatedContract)] =
    (str("contract_id")
      ~ int("template_id")
      ~ array[Int]("flat_event_witnesses")
      ~ byteArray("create_argument")
      ~ int("create_argument_compression").?
      ~ timestampFromMicros("ledger_effective_time")
      ~ str("create_agreement_text").?
      ~ array[Int]("create_signatories")
      ~ byteArray("create_key_value").?
      ~ int("create_key_value_compression").?
      ~ array[Int]("create_key_maintainers").?
      ~ byteArray("driver_metadata").?)
      .map {
        case coid ~ internalTemplateId ~ flatEventWitnesses ~ createArgument ~ createArgumentCompression ~ ledgerEffectiveTime ~ agreementText ~ signatories ~ createKey ~ createKeyCompression ~ keyMaintainers ~ driverMetadata =>
          ContractId.assertFromString(coid) -> RawCreatedContract(
            templateId = stringInterning.templateId.unsafe.externalize(internalTemplateId),
            flatEventWitnesses =
              flatEventWitnesses.view.map(stringInterning.party.externalize).toSet,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            ledgerEffectiveTime = ledgerEffectiveTime,
            agreementText = agreementText,
            signatories = signatories.view.map(i => stringInterning.party.externalize(i)).toSet,
            createKey = createKey,
            createKeyCompression = createKeyCompression,
            keyMaintainers =
              keyMaintainers.map(_.view.map(i => stringInterning.party.externalize(i)).toSet),
            driverMetadata = driverMetadata,
          )
      }

  override def createdContracts(contractIds: Seq[ContractId], before: Offset)(
      connection: Connection
  ): Map[ContractId, RawCreatedContract] =
    if (contractIds.isEmpty) Map.empty
    else {
      import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
      SQL"""
         SELECT
           contract_id,
           template_id,
           flat_event_witnesses,
           create_argument,
           create_argument_compression,
           ledger_effective_time,
           create_agreement_text,
           create_signatories,
           create_key_value,
           create_key_value_compression,
           create_key_maintainers,
           driver_metadata
         FROM participant_events_create
         WHERE
           contract_id ${queryStrategy.anyOfStrings(contractIds.map(_.coid))}
           AND event_offset <= $before"""
        .as(rawCreatedContractRowParser.*)(connection)
        .toMap
    }

  override def assignedContracts(contractIds: Seq[ContractId])(
      connection: Connection
  ): Map[ContractId, RawCreatedContract] =
    if (contractIds.isEmpty) Map.empty
    else {
      import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
      val ledgerEndOffset = ledgerEndCache()._1
      SQL"""
         WITH min_event_sequential_ids_of_assign AS (
             SELECT MIN(event_sequential_id) min_event_sequential_id
             FROM participant_events_assign
             WHERE
               contract_id ${queryStrategy.anyOfStrings(contractIds.map(_.coid))}
               AND event_offset <= $ledgerEndOffset
             GROUP BY contract_id
           )
         SELECT
           contract_id,
           template_id,
           flat_event_witnesses,
           create_argument,
           create_argument_compression,
           ledger_effective_time,
           create_agreement_text,
           create_signatories,
           create_key_value,
           create_key_value_compression,
           create_key_maintainers,
           driver_metadata
         FROM participant_events_assign, min_event_sequential_ids_of_assign
         WHERE
           event_sequential_id = min_event_sequential_ids_of_assign.min_event_sequential_id"""
        .as(rawCreatedContractRowParser.*)(connection)
        .toMap
    }

  private val rawContractRowParser: RowParser[ContractStorageBackend.RawContract] =
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
    import com.digitalasset.canton.platform.store.backend.Conversions.ContractIdToStatement
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
      resultSetParser = rawContractRowParser.singleOpt,
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
