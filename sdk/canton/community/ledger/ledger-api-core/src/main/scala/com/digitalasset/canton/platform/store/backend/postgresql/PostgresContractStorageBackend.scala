// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import anorm.SqlParser.{int, long}
import anorm.{SqlStringInterpolation, ~}
import com.digitalasset.canton.platform.store.backend.Conversions.{contractId, hashFromHexString}
import com.digitalasset.canton.platform.store.backend.PersistentEventType
import com.digitalasset.canton.platform.store.backend.common.ContractStorageBackendTemplate
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
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

class PostgresContractStorageBackend(
    stringInterning: StringInterning,
    ledgerEndCache: LedgerEndCache,
) extends ContractStorageBackendTemplate(PostgresQueryStrategy, stringInterning, ledgerEndCache) {

  override def keyStatesNew(keys: Seq[Key], validAtEventSeqId: Long)(
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

  override def lastActivationsNew(synchronizerContracts: Iterable[(SynchronizerId, Long)])(
      connection: Connection
  ): Map[(SynchronizerId, Long), Long] =
    ledgerEndCache()
      .map { ledgerEnd =>
        val inputWithIndex = synchronizerContracts.zipWithIndex
        val inputIndexes: Array[java.lang.Integer] = inputWithIndex.iterator.map {
          case (_, index) =>
            Int.box(index)
        }.toArray
        val inputSynchronizerIds: Array[java.lang.Integer] = inputWithIndex.iterator.map {
          case ((synchronizerId, _), _) =>
            Int.box(stringInterning.synchronizerId.internalize(synchronizerId))
        }.toArray
        val inputInternalContractIds: Array[java.lang.Long] = inputWithIndex.iterator.map {
          case ((_, contractId), _) => Long.box(contractId)
        }.toArray
        val resultParser = int("result_index") ~ long("result_event_sequential_id") map {
          case index ~ resultEventSeqId => index -> resultEventSeqId
        }
        val results = SQL"""
          SELECT input.index as result_index, activate_evs.event_sequential_id as result_event_sequential_id
          FROM UNNEST($inputIndexes, $inputSynchronizerIds, $inputInternalContractIds) AS input(index, synchronizer_id, internal_contract_id)
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
          ) activate_evs"""
          .asVectorOf(resultParser)(connection)
          .toMap
        inputWithIndex.iterator.flatMap { case (synCon, index) =>
          results.get(index).map(synCon -> _)
        }.toMap
      }
      .getOrElse(Map.empty)

  override final def supportsBatchKeyStateLookups: Boolean = true

  override def keyStates(keys: Seq[Key], validAtEventSeqId: Long)(
      connection: Connection
  ): Map[Key, KeyState] = if (keys.isEmpty) Map()
  else {
    val resultParser =
      (contractId("contract_id") ~ hashFromHexString("create_key_hash")).map { case cId ~ hash =>
        hash -> KeyAssigned(cId)
      }.*

    // efficient adaption of the "single lookup query" using postgres specific syntax
    // the unnest will expand the keys into a temporary table. the cross join lateral runs the
    // efficient single key lookup query on each row in the temporary table. this efficient
    // single key lookup will perform a backwards index scan
    // implementing this for oracle is left to the reader as an exercise.
    val res = SQL"""
  WITH last_contract_key_create AS (
      SELECT p.contract_id, p.create_key_hash, p.flat_event_witnesses
      FROM UNNEST(${keys.view.map(_.hash.bytes.toHexString).toArray[String]}) AS k(create_key_hash)
    CROSS JOIN LATERAL (
        SELECT *
        FROM lapi_events_create p
        WHERE p.create_key_hash = k.create_key_hash
          AND p.event_sequential_id <= $validAtEventSeqId
        ORDER BY p.event_sequential_id DESC
        LIMIT 1
    ) p
  )
  SELECT contract_id, create_key_hash, flat_event_witnesses
  FROM last_contract_key_create
  WHERE NOT EXISTS (
      SELECT 1
      FROM lapi_events_consuming_exercise
      WHERE contract_id = last_contract_key_create.contract_id
      AND event_sequential_id <= $validAtEventSeqId
  )"""
      .as(resultParser)(connection)
      .toMap
    keys.map { case (key) =>
      (key, res.getOrElse(key.hash, KeyUnassigned))
    }.toMap
  }

  override def lastActivations(
      synchronizerContracts: Iterable[(SynchronizerId, ContractId)]
  )(
      connection: Connection
  ): Map[(SynchronizerId, ContractId), Long] = ledgerEndCache()
    .map { ledgerEnd =>
      val inputWithIndex = synchronizerContracts.zipWithIndex
      val inputIndexes: Array[java.lang.Integer] = inputWithIndex.iterator.map { case (_, index) =>
        Int.box(index)
      }.toArray
      val inputSynchronizerIds: Array[java.lang.Integer] = inputWithIndex.iterator.map {
        case ((synchronizerId, _), _) =>
          Int.box(stringInterning.synchronizerId.internalize(synchronizerId))
      }.toArray
      // need to override the default to not allow anorm to guess incorrectly the array type
      import PostgresQueryStrategy.ArrayByteaToStatement
      val inputContractIds: Array[Array[Byte]] = inputWithIndex.iterator.map {
        case ((_, contractId), _) => contractId.toBytes.toByteArray
      }.toArray
      val resultParser = int("result_index") ~ long("result_event_sequential_id") map {
        case index ~ resultEventSeqId => index -> resultEventSeqId
      }
      val resultsFromAssign = SQL"""
        SELECT input.index as result_index, assign_evs.event_sequential_id as result_event_sequential_id
        FROM UNNEST($inputIndexes, $inputSynchronizerIds, $inputContractIds) AS input(index, synchronizer_id, contract_id)
        CROSS JOIN LATERAL (
          SELECT *
          FROM lapi_events_assign assign_evs
          WHERE assign_evs.contract_id = input.contract_id
          AND assign_evs.target_synchronizer_id = input.synchronizer_id
          AND assign_evs.event_sequential_id <= ${ledgerEnd.lastEventSeqId}
          ORDER BY assign_evs.event_sequential_id DESC
          LIMIT 1
        ) assign_evs"""
        .asVectorOf(resultParser)(connection)
        .toMap
      val resultsFromCreate = SQL"""
        SELECT input.index as result_index, create_evs.event_sequential_id as result_event_sequential_id
        FROM UNNEST($inputIndexes, $inputSynchronizerIds, $inputContractIds) AS input(index, synchronizer_id, contract_id)
        CROSS JOIN LATERAL (
          SELECT *
          FROM lapi_events_create create_evs
          WHERE create_evs.contract_id = input.contract_id
          AND create_evs.synchronizer_id = input.synchronizer_id
          AND create_evs.event_sequential_id <= ${ledgerEnd.lastEventSeqId}
          ORDER BY create_evs.event_sequential_id DESC
          LIMIT 1
        ) create_evs"""
        .asVectorOf(resultParser)(connection)
        .toMap
      inputWithIndex.iterator.flatMap { case (synCon, index) =>
        List(resultsFromAssign, resultsFromCreate)
          .flatMap(_.get(index))
          .maxOption
          .map(synCon -> _)
      }.toMap
    }
    .getOrElse(Map.empty)
}
