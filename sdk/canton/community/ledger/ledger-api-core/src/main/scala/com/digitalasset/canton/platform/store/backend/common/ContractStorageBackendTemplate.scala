// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{bool, long}
import anorm.~
import com.digitalasset.canton.platform.Key
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.`SimpleSql ops`
import com.digitalasset.canton.platform.store.backend.{ContractStorageBackend, PersistentEventType}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
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
  override def keyStates(keys: Seq[Key], validAtEventSeqId: Long)(
      connection: Connection
  ): Map[Key, Long] =
    keys.iterator
      .flatMap(key =>
        keyState(key, validAtEventSeqId)(connection)
          .map(key -> _)
      )
      .toMap

  /** Sequential lookup of key states */
  override def keyState(key: Key, validAtEventSeqId: Long)(
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

  override def activeContracts(internalContractIds: Seq[Long], beforeEventSeqId: Long)(
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

  override def lastActivations(synchronizerContracts: Iterable[(SynchronizerId, Long)])(
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
}
