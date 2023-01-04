// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import anorm.{Row, SimpleSql}
import com.daml.platform.ContractId
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.ContractStorageBackendTemplate
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

class H2ContractStorageBackend(ledgerEndCache: LedgerEndCache, stringInterning: StringInterning)
    extends ContractStorageBackendTemplate(H2QueryStrategy, ledgerEndCache, stringInterning) {

  override def activeContractSqlLiteral(
      contractId: ContractId,
      treeEventWitnessesClause: CompositeSql,
      resultColumns: List[String],
      coalescedColumns: String,
  ): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.ContractIdToStatement
    val lastEventSequentialId = ledgerEndCache()._2
    SQL"""  WITH archival_event AS (
               SELECT 1
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

}
