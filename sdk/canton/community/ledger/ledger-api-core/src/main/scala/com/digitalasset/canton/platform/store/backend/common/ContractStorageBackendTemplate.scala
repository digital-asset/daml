// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{bool, long}
import anorm.{RowParser, ~}
import com.digitalasset.canton.platform.store.backend.Conversions.contractId
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.`SimpleSql ops`
import com.digitalasset.canton.platform.store.backend.{ContractStorageBackend, PersistentEventType}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{ContractId, Key}
import com.digitalasset.canton.topology.SynchronizerId

import java.sql.Connection

class ContractStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    stringInterning: StringInterning,
    ledgerEndCache: LedgerEndCache,
) extends ContractStorageBackend {

  /** Batch lookup of key states
    *
    * If the backend does not support batch lookups, the implementation will fall back to sequential
    * lookups
    */
  override def keyStatesNew(keys: Seq[Key], validAtEventSeqId: Long)(
      connection: Connection
  ): Map[Key, Long] =
    keys.iterator
      .flatMap(key =>
        keyStateNew(key, validAtEventSeqId)(connection)
          .map(key -> _)
      )
      .toMap

  /** Sequential lookup of key states */
  override def keyStateNew(key: Key, validAtEventSeqId: Long)(
      connection: Connection
  ): Option[Long] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.HashToStatement
    SQL"""
         WITH last_contract_key_create AS (
                SELECT lapi_events_activate_contract.*
                  FROM lapi_events_activate_contract
                 WHERE create_key_hash = ${key.hash}
                   AND event_sequential_id <= $validAtEventSeqId
                 ORDER BY event_sequential_id DESC
                 FETCH NEXT 1 ROW ONLY
              )
         SELECT internal_contract_id
           FROM last_contract_key_create
         WHERE NOT EXISTS
                (SELECT 1
                   FROM lapi_events_deactivate_contract
                  WHERE
                    internal_contract_id = last_contract_key_create.internal_contract_id
                    AND event_sequential_id <= $validAtEventSeqId
                    AND event_type = ${PersistentEventType.ConsumingExercise.asInt}
                )"""
      .as(long("internal_contract_id").singleOpt)(connection)
  }

  override def activeContractsNew(internalContractIds: Seq[Long], beforeEventSeqId: Long)(
      connection: Connection
  ): Map[Long, Boolean] =
    if (internalContractIds.isEmpty) Map.empty
    else {
      SQL"""
       SELECT
         internal_contract_id,
         NOT EXISTS (
           SELECT 1
           FROM lapi_events_deactivate_contract
           WHERE
             internal_contract_id = lapi_events_activate_contract.internal_contract_id
             AND event_sequential_id <= $beforeEventSeqId
             AND event_type = ${PersistentEventType.ConsumingExercise.asInt}
           LIMIT 1
         ) active
       FROM lapi_events_activate_contract
       WHERE
         internal_contract_id ${queryStrategy.anyOf(internalContractIds)}
         AND event_sequential_id <= $beforeEventSeqId"""
        .asVectorOf(long("internal_contract_id") ~ bool("active"))(connection)
        .view
        .map { case internalContractId ~ active =>
          internalContractId -> active
        }
        .toMap
    }

  override def lastActivationsNew(synchronizerContracts: Iterable[(SynchronizerId, Long)])(
      connection: Connection
  ): Map[(SynchronizerId, Long), Long] =
    ledgerEndCache()
      .map { ledgerEnd =>
        synchronizerContracts.iterator.flatMap { case (synchronizerId, internalContractId) =>
          val internedSynchronizerId = stringInterning.synchronizerId.internalize(synchronizerId)
          SQL"""
          SELECT event_sequential_id
          FROM lapi_events_activate_contract as activate
          WHERE
            internal_contract_id = $internalContractId AND
            event_sequential_id <= ${ledgerEnd.lastEventSeqId} AND
            EXISTS ( -- subquery for triggering (event_sequential_id) INCLUDE (synchronizer_id) index usage
              SELECT 1
              FROM lapi_events_activate_contract as activate2
              WHERE
                activate2.event_sequential_id = activate.event_sequential_id AND
                activate2.synchronizer_id = $internedSynchronizerId
            )
          ORDER BY event_sequential_id DESC
          LIMIT 1"""
            .as(long("event_sequential_id").singleOpt)(connection)
            .map((synchronizerId, internalContractId) -> _)
        }.toMap
      }
      .getOrElse(Map.empty)

  override def supportsBatchKeyStateLookups: Boolean = false

  override def keyStates(keys: Seq[Key], validAtEventSeqId: Long)(
      connection: Connection
  ): Map[Key, KeyState] = keys.map(key => key -> keyState(key, validAtEventSeqId)(connection)).toMap

  override def keyState(key: Key, validAtEventSeqId: Long)(connection: Connection): KeyState = {
    val resultParser = contractId("contract_id").map(KeyAssigned.apply).singleOpt
    import com.digitalasset.canton.platform.store.backend.Conversions.HashToStatement
    SQL"""
         WITH last_contract_key_create AS (
                SELECT lapi_events_create.*
                  FROM lapi_events_create
                 WHERE create_key_hash = ${key.hash}
                   AND event_sequential_id <= $validAtEventSeqId
                   AND length(flat_event_witnesses) > 1 -- exclude participant divulgence and transients
                 ORDER BY event_sequential_id DESC
                 FETCH NEXT 1 ROW ONLY
              )
         SELECT contract_id
           FROM last_contract_key_create
         WHERE NOT EXISTS
                (SELECT 1
                   FROM lapi_events_consuming_exercise
                  WHERE
                    contract_id = last_contract_key_create.contract_id
                    AND event_sequential_id <= $validAtEventSeqId
                )"""
      .as(resultParser)(connection)
      .getOrElse(KeyUnassigned)
  }

  private val contractIdRowParser: RowParser[ContractId] =
    contractId("contract_id")

  override def archivedContracts(contractIds: Seq[ContractId], beforeEventSeqId: Long)(
      connection: Connection
  ): Set[ContractId] =
    if (contractIds.isEmpty) Set.empty
    else {
      SQL"""
       SELECT contract_id
       FROM lapi_events_consuming_exercise
       WHERE
         contract_id ${queryStrategy.anyOfBinary(contractIds.map(_.toBytes.toByteArray))}
         AND event_sequential_id <= $beforeEventSeqId
         AND length(flat_event_witnesses) > 1 -- exclude participant divulgence and transients"""
        .as(contractIdRowParser.*)(connection)
        .toSet
    }

  override def createdContracts(contractIds: Seq[ContractId], beforeEventSeqId: Long)(
      connection: Connection
  ): Set[ContractId] =
    if (contractIds.isEmpty) Set.empty
    else {
      SQL"""
         SELECT
           contract_id
         FROM lapi_events_create
         WHERE
           contract_id ${queryStrategy.anyOfBinary(contractIds.map(_.toBytes.toByteArray))}
           AND event_sequential_id <= $beforeEventSeqId
           AND length(flat_event_witnesses) > 1 -- exclude participant divulgence and transients"""
        .as(contractIdRowParser.*)(connection)
        .toSet
    }

  override def assignedContracts(
      contractIds: Seq[ContractId],
      beforeEventSeqId: Long,
  )(
      connection: Connection
  ): Set[ContractId] =
    if (contractIds.isEmpty) Set.empty
    else {
      SQL"""
         WITH min_event_sequential_ids_of_assign AS (
             SELECT MIN(event_sequential_id) min_event_sequential_id
             FROM lapi_events_assign
             WHERE
               contract_id ${queryStrategy.anyOfBinary(contractIds.map(_.toBytes.toByteArray))}
               AND event_sequential_id <= $beforeEventSeqId
             GROUP BY contract_id
           )
         SELECT
           contract_id
         FROM lapi_events_assign, min_event_sequential_ids_of_assign
         WHERE
           event_sequential_id = min_event_sequential_ids_of_assign.min_event_sequential_id"""
        .as(contractIdRowParser.*)(connection)
        .toSet
    }

  override def lastActivations(
      synchronizerContracts: Iterable[(SynchronizerId, ContractId)]
  )(
      connection: Connection
  ): Map[(SynchronizerId, ContractId), Long] = ledgerEndCache()
    .map { ledgerEnd =>
      synchronizerContracts.iterator.flatMap { case (synchronizerId, contractId) =>
        val internedSynchronizerId = stringInterning.synchronizerId.internalize(synchronizerId)
        val createEventSeqId = SQL"""
            SELECT event_sequential_id
            FROM lapi_events_create
            WHERE
              contract_id = ${contractId.toBytes.toByteArray} AND
              synchronizer_id = $internedSynchronizerId AND
              event_sequential_id <= ${ledgerEnd.lastEventSeqId}
              -- not checking here the fact of activation (flat_event_witnesses) because it is invalid to have non-divulged deactivation for non-divulged create. Transients won't be searched for in the first place.
            LIMIT 1"""
          .as(long("event_sequential_id").singleOpt)(connection)
        val assignEventSeqId = SQL"""
            SELECT event_sequential_id
            FROM lapi_events_assign
            WHERE
              contract_id = ${contractId.toBytes.toByteArray} AND
              target_synchronizer_id = $internedSynchronizerId AND
              event_sequential_id <= ${ledgerEnd.lastEventSeqId}
            ORDER BY event_sequential_id DESC
            LIMIT 1"""
          .as(long("event_sequential_id").singleOpt)(connection)
        List(createEventSeqId, assignEventSeqId).flatten.maxOption
          .map((synchronizerId, contractId) -> _)
      }.toMap
    }
    .getOrElse(Map.empty)
}
