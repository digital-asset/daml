// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import anorm.{Row, SimpleSql}
import com.daml.platform.store.appendonlydao.events.{ContractId, Key}
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.ContractStorageBackend
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interning.StringInterning

class NonValidatingContractStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
) extends ContractStorageBackendTemplate(queryStrategy, ledgerEndCache, stringInterning) {

  override def keyState(key: Key, validAt: Long)(
      connection: Connection
  ): LedgerDaoContractsReader.KeyState =
    LedgerDaoContractsReader.KeyUnassigned

  override def contractState(contractId: ContractId, before: Long)(
      connection: Connection
  ): Option[ContractStorageBackend.RawContractState] = {
    import com.daml.platform.store.Conversions.ContractIdToStatement

    /** Lookup only Create events ignoring Archive events */
    SQL"""
           SELECT
             template_id,
             flat_event_witnesses,
             create_argument,
             create_argument_compression,
             event_kind,
             ledger_effective_time
           FROM participant_events
           WHERE
             contract_id = $contractId
             AND event_sequential_id <= $before
             AND event_kind = 10
           ORDER BY event_sequential_id DESC
           FETCH NEXT 1 ROW ONLY"""
      .as(fullDetailsContractRowParser.singleOpt)(connection)
  }

  override protected def activeContractSqlLiteral(
      contractId: ContractId,
      treeEventWitnessesClause: CompositeSql,
      resultColumns: List[String],
      coalescedColumns: String,
  ): SimpleSql[Row] = {
    val lastEventSequentialId = ledgerEndCache()._2
    import com.daml.platform.store.Conversions.ContractIdToStatement
    SQL"""  WITH create_event AS (
               SELECT contract_id, #${resultColumns.mkString(", ")}
                 FROM participant_events
                WHERE contract_id = $contractId
                  AND event_kind = 10  -- create
                  AND event_sequential_id <= $lastEventSequentialId
                  AND $treeEventWitnessesClause
                FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
             ),
             -- no visibility check, as it is used to backfill missing template_id and create_arguments for divulged contracts
             create_event_unrestricted AS (
               SELECT contract_id, #${resultColumns.mkString(", ")}
                 FROM participant_events
                WHERE contract_id = $contractId
                  AND event_kind = 10  -- create
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
                 FROM participant_events divulgence_events LEFT OUTER JOIN create_event_unrestricted ON (divulgence_events.contract_id = create_event_unrestricted.contract_id)
                WHERE divulgence_events.contract_id = $contractId -- restrict to aid query planner
                  AND divulgence_events.event_kind = 0 -- divulgence
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
         FETCH NEXT 1 ROW ONLY"""
  }
}
