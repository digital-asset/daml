// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{array, byteArray, int, str}
import anorm.{RowParser, ~}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.{
  RawArchivedContract,
  RawCreatedContract,
}
import com.digitalasset.canton.platform.store.backend.Conversions.{contractId, timestampFromMicros}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{ContractId, Key}

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
                SELECT lapi_events_create.*
                  FROM lapi_events_create
                 WHERE create_key_hash = ${key.hash}
                   AND event_offset <= $validAt
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
       FROM lapi_events_consuming_exercise
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
         FROM lapi_events_create
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
             FROM lapi_events_assign
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
         FROM lapi_events_assign, min_event_sequential_ids_of_assign
         WHERE
           event_sequential_id = min_event_sequential_ids_of_assign.min_event_sequential_id"""
        .as(rawCreatedContractRowParser.*)(connection)
        .toMap
    }
}
