// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{byteArray, int, long}
import anorm.{RowParser, ~}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.{
  RawArchivedContract,
  RawCreatedContract,
}
import com.digitalasset.canton.platform.store.backend.Conversions.{
  OffsetToStatement,
  contractId,
  parties,
  timestampFromMicros,
}
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

  override def keyStates(keys: Seq[Key], validAt: Offset)(
      connection: Connection
  ): Map[Key, KeyState] = keys.map(key => key -> keyState(key, validAt)(connection)).toMap

  override def keyState(key: Key, validAt: Offset)(connection: Connection): KeyState = {
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
                   AND event_offset <= $validAt
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
                    AND event_offset <= $validAt
                )"""
      .as(resultParser)(connection)
      .getOrElse(KeyUnassigned)
  }

  private val archivedContractRowParser: RowParser[(ContractId, RawArchivedContract)] =
    (contractId("contract_id") ~ parties(stringInterning)("flat_event_witnesses"))
      .map { case coid ~ flatEventWitnesses =>
        coid -> RawArchivedContract(
          flatEventWitnesses = flatEventWitnesses.toSet
        )
      }

  override def archivedContracts(contractIds: Seq[ContractId], before: Offset)(
      connection: Connection
  ): Map[ContractId, RawArchivedContract] =
    if (contractIds.isEmpty) Map.empty
    else {
      SQL"""
       SELECT contract_id, flat_event_witnesses
       FROM lapi_events_consuming_exercise
       WHERE
         contract_id ${queryStrategy.anyOfBinary(contractIds.map(_.toBytes.toByteArray))}
         AND event_offset <= $before
         AND length(flat_event_witnesses) > 1 -- exclude participant divulgence and transients"""
        .as(archivedContractRowParser.*)(connection)
        .toMap
    }

  private val rawCreatedContractRowParser
      : RowParser[(ContractId, ContractStorageBackend.RawCreatedContract)] =
    (contractId("contract_id")
      ~ int("template_id")
      ~ int("package_id")
      ~ parties(stringInterning)("flat_event_witnesses")
      ~ byteArray("create_argument")
      ~ int("create_argument_compression").?
      ~ timestampFromMicros("ledger_effective_time")
      ~ parties(stringInterning)("create_signatories")
      ~ byteArray("create_key_value").?
      ~ int("create_key_value_compression").?
      ~ parties(stringInterning)("create_key_maintainers").?
      ~ byteArray("authentication_data"))
      .map {
        case coid ~ internedTemplateId ~ internedPackageId ~ flatEventWitnesses ~ createArgument ~ createArgumentCompression ~ ledgerEffectiveTime ~ signatories ~ createKey ~ createKeyCompression ~ keyMaintainers ~ authenticationData =>
          coid -> RawCreatedContract(
            templateId = stringInterning.templateId.unsafe.externalize(internedTemplateId),
            packageId = stringInterning.packageId.unsafe.externalize(internedPackageId),
            flatEventWitnesses = flatEventWitnesses.toSet,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            ledgerEffectiveTime = ledgerEffectiveTime,
            signatories = signatories.toSet,
            createKey = createKey,
            createKeyCompression = createKeyCompression,
            keyMaintainers = keyMaintainers.map(_.toSet),
            authenticationData = authenticationData,
          )
      }

  override def createdContracts(contractIds: Seq[ContractId], before: Offset)(
      connection: Connection
  ): Map[ContractId, RawCreatedContract] =
    if (contractIds.isEmpty) Map.empty
    else {
      SQL"""
         SELECT
           contract_id,
           template_id,
           package_id,
           flat_event_witnesses,
           create_argument,
           create_argument_compression,
           ledger_effective_time,
           create_signatories,
           create_key_value,
           create_key_value_compression,
           create_key_maintainers,
           authentication_data
         FROM lapi_events_create
         WHERE
           contract_id ${queryStrategy.anyOfBinary(contractIds.map(_.toBytes.toByteArray))}
           AND event_offset <= $before
           AND length(flat_event_witnesses) > 1 -- exclude participant divulgence and transients"""
        .as(rawCreatedContractRowParser.*)(connection)
        .toMap
    }

  override def assignedContracts(
      contractIds: Seq[ContractId],
      before: Offset,
  )(
      connection: Connection
  ): Map[ContractId, RawCreatedContract] =
    if (contractIds.isEmpty) Map.empty
    else {
      SQL"""
         WITH min_event_sequential_ids_of_assign AS (
             SELECT MIN(event_sequential_id) min_event_sequential_id
             FROM lapi_events_assign
             WHERE
               contract_id ${queryStrategy.anyOfBinary(contractIds.map(_.toBytes.toByteArray))}
               AND event_offset <= $before
             GROUP BY contract_id
           )
         SELECT
           contract_id,
           template_id,
           package_id,
           flat_event_witnesses,
           create_argument,
           create_argument_compression,
           ledger_effective_time,
           create_signatories,
           create_key_value,
           create_key_value_compression,
           create_key_maintainers,
           authentication_data
         FROM lapi_events_assign, min_event_sequential_ids_of_assign
         WHERE
           event_sequential_id = min_event_sequential_ids_of_assign.min_event_sequential_id"""
        .as(rawCreatedContractRowParser.*)(connection)
        .toMap
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
