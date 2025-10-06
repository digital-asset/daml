// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.long
import anorm.{RowParser, ~}
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.Conversions.{contractId, parties}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
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

  override def supportsBatchKeyStateLookups: Boolean = false

  override def keyStates(keys: Seq[Key], validAtEventSeqId: Long)(
      connection: Connection
  ): Map[Key, KeyState] = keys.map(key => key -> keyState(key, validAtEventSeqId)(connection)).toMap

  override def keyState(key: Key, validAtEventSeqId: Long)(connection: Connection): KeyState = {
    val resultParser =
      (contractId("contract_id") ~ parties(stringInterning)("flat_event_witnesses")).map {
        case cId ~ stakeholders =>
          KeyAssigned(cId, stakeholders.toSet)
      }.singleOpt
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
         SELECT contract_id, flat_event_witnesses
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
