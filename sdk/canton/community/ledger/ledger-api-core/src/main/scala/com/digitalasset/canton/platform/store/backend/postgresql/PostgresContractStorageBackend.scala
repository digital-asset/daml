// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import anorm.SqlParser.long
import anorm.~
import com.digitalasset.canton.platform.Key
import com.digitalasset.canton.platform.store.backend.Conversions.hashFromHexString
import com.digitalasset.canton.platform.store.backend.PersistentEventType
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.backend.common.{
  ContractStorageBackendTemplate,
  QueryStrategy,
}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.SynchronizerId

import java.sql.Connection

class PostgresContractStorageBackend(
    stringInterning: StringInterning,
    ledgerEndCache: LedgerEndCache,
) extends ContractStorageBackendTemplate(PostgresQueryStrategy, stringInterning, ledgerEndCache) {

  override def keyStates(keys: Seq[Key], validAtEventSeqId: Long)(
      connection: Connection
  ): Map[Key, Long] =
    if (keys.isEmpty) Map.empty
    else {
      val res = SQL"""
        WITH last_contract_key_create AS (
          SELECT p.internal_contract_id, p.create_key_hash
          FROM UNNEST(${keys.view
          .map(_.hash.bytes.toHexString)
          .toArray[String]}) AS k(create_key_hash)
          CROSS JOIN LATERAL (
            SELECT *
            FROM lapi_events_activate_contract p
            WHERE
              p.create_key_hash = k.create_key_hash AND
              p.event_sequential_id <= $validAtEventSeqId
            ORDER BY p.event_sequential_id DESC
            LIMIT 1
          ) p
        )
        SELECT internal_contract_id, create_key_hash
        FROM last_contract_key_create
        WHERE NOT EXISTS (
          SELECT 1
          FROM lapi_events_deactivate_contract
          WHERE
            internal_contract_id = last_contract_key_create.internal_contract_id AND
            event_sequential_id <= $validAtEventSeqId AND
            event_type = ${PersistentEventType.ConsumingExercise.asInt}
        )"""
        .asVectorOf(
          long("internal_contract_id") ~ hashFromHexString("create_key_hash") map {
            case internalContractId ~ hash => hash -> internalContractId
          }
        )(connection)
        .toMap
      keys
        .flatMap(key => res.get(key.hash).map(key -> _))
        .toMap
    }

  override def lastActivations(synchronizerContracts: Iterable[(SynchronizerId, Long)])(
      connection: Connection
  ): Map[(SynchronizerId, Long), Long] =
    ledgerEndCache()
      .map { ledgerEnd =>
        val inputWithIndex = synchronizerContracts.zipWithIndex
        def toArrayLiteral(values: Iterable[Any]): String = values.mkString("ARRAY[", ", ", "]")
        val indexArrayLiteral = toArrayLiteral(inputWithIndex.view.map(_._2))
        val synchronizerIdArrayLiteral = toArrayLiteral(
          inputWithIndex.view.map(_._1._1).map(stringInterning.synchronizerId.internalize)
        )
        val internalContractIdArrayLiteral = toArrayLiteral(inputWithIndex.view.map(_._1._2))
        // Resorting here to non-prepared statement as the combination of prepared statement and unnest and cross lateral join produced very inefficient query plans with PostgreSQL.
        // For Future reference:
        //   * Wrong query plan involved traversing the event_sequential_id index backwards in a index scan and eliminating candidates with filters on table itself (the good plan is the descending index only scan with index condition over the contract ID)
        //   * Query plans without prepared statement results in an efficient plan in tests
        //   * Only the prepared statement via JDBC resulted in inefficient plans (creating prepared statements for example via psql tool with PREPARE was not exhibiting the same problem)
        val results = QueryStrategy
          .plainJdbcQuery(s"""
          SELECT input.index as result_index, activate_evs.event_sequential_id as result_event_sequential_id
          FROM UNNEST($indexArrayLiteral, $synchronizerIdArrayLiteral, $internalContractIdArrayLiteral) AS input(index, synchronizer_id, internal_contract_id)
          CROSS JOIN LATERAL (
            SELECT *
            FROM lapi_events_activate_contract activate_evs
            WHERE activate_evs.internal_contract_id = input.internal_contract_id
            AND activate_evs.event_sequential_id <= ${ledgerEnd.lastEventSeqId}
            AND EXISTS ( -- subquery for triggering (event_sequential_id) INCLUDE (synchronizer_id) index usage
              SELECT 1
              FROM lapi_events_activate_contract as activate_evs2
              WHERE
                activate_evs2.event_sequential_id = activate_evs.event_sequential_id AND
                activate_evs2.synchronizer_id = input.synchronizer_id
            )
            ORDER BY activate_evs.event_sequential_id DESC
            LIMIT 1
          ) activate_evs""")(resultSet =>
            (
              resultSet.getInt("result_index"),
              resultSet.getLong("result_event_sequential_id"),
            )
          )(connection)
          .toMap
        inputWithIndex.iterator.flatMap { case (synCon, index) =>
          results.get(index).map(synCon -> _)
        }.toMap
      }
      .getOrElse(Map.empty)

  override final def supportsBatchKeyStateLookups: Boolean = true
}
