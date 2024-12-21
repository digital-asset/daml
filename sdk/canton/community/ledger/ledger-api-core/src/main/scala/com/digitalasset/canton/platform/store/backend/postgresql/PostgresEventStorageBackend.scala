// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import anorm.SqlParser.*
import anorm.~
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.UnassignProperties
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.EventStorageBackendTemplate
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

import java.sql.Connection

class PostgresEventStorageBackend(
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    parameterStorageBackend: ParameterStorageBackend,
    loggerFactory: NamedLoggerFactory,
) extends EventStorageBackendTemplate(
      queryStrategy = PostgresQueryStrategy,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      participantAllDivulgedContractsPrunedUpToInclusive =
        parameterStorageBackend.participantAllDivulgedContractsPrunedUpToInclusive,
      loggerFactory = loggerFactory,
    ) {

  override def lookupAssignSequentialIdBy(
      unassignProperties: Iterable[UnassignProperties]
  )(connection: Connection): Map[UnassignProperties, Long] =
    if (unassignProperties.isEmpty) Map.empty
    else
      {

        val (contractIds, domainIds, sequentialIds) = unassignProperties.view.flatMap { prop =>
          stringInterning.domainId.unsafe
            .tryInternalize(prop.domainId)
            .map(internedDomainId => (prop.contractId, internedDomainId, prop.sequentialId))
        }.unzip3

        val contractIdsJavaArray: Array[String] = contractIds.toArray
        val domainIdsJavaArray: Array[Integer] = domainIds.map(Int.box).toArray
        val sequentialIdsJavaArray: Array[java.lang.Long] = sequentialIds.map(Long.box).toArray

        SQL"""
        SELECT assign_evs.event_sequential_id AS assign_event_sequential_id, unassign_evs.contract_id, unassign_evs.domain_id, unassign_evs.event_sequential_id AS unassign_event_sequential_id
        FROM UNNEST($contractIdsJavaArray, $domainIdsJavaArray, $sequentialIdsJavaArray) AS unassign_evs(contract_id, domain_id, event_sequential_id)
        CROSS JOIN LATERAL (
          SELECT *
          FROM lapi_events_assign assign_evs
          WHERE assign_evs.contract_id = unassign_evs.contract_id
          AND assign_evs.target_domain_id = unassign_evs.domain_id
          AND assign_evs.event_sequential_id < unassign_evs.event_sequential_id
          ORDER BY assign_evs.event_sequential_id DESC
          LIMIT 1
        ) assign_evs
      """
          .asVectorOf(
            long("assign_event_sequential_id")
              ~ str("contract_id")
              ~ int("domain_id")
              ~ long("unassign_event_sequential_id")
              map { case foundSeqId ~ contractId ~ internedDomainId ~ queriedSeqId =>
                val domainId = stringInterning.domainId.unsafe.externalize(internedDomainId)
                (
                  UnassignProperties(contractId, domainId, queriedSeqId),
                  foundSeqId,
                )
              }
          )(connection)
      }.toMap
}
