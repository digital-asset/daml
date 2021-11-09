// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.sql.Connection
import java.time.Instant

import anorm.SqlStringInterpolation
import com.daml.ledger.api.domain.PartyDetails
import com.daml.platform.apiserver.execution.MissingContracts
import com.daml.platform.store.Conversions._
import com.daml.platform.store.appendonlydao.JdbcLedgerDao

import scala.util.{Failure, Success, Try}

private[events] object ContractsTable extends PostCommitValidationData {

  override final def lookupContractKeyGlobally(
      key: Key
  )(implicit connection: Connection): Option[ContractId] =
    SQL"""
  WITH last_contract_key_create AS (
         SELECT participant_events.*
           FROM participant_events, parameters
          WHERE event_kind = 10 -- create
            AND create_key_hash = ${key.hash}
            AND event_sequential_id <= parameters.ledger_end_sequential_id
          ORDER BY event_sequential_id DESC
          LIMIT 1
       )
  SELECT contract_id
    FROM last_contract_key_create -- creation only, as divulged contracts cannot be fetched by key
   WHERE NOT EXISTS
         (SELECT 1
            FROM participant_events, parameters
           WHERE event_kind = 20 -- consuming exercise
             AND event_sequential_id <= parameters.ledger_end_sequential_id
             AND contract_id = last_contract_key_create.contract_id
         );
       """
      .as(contractId("contract_id").singleOpt)

  // TODO append-only: revisit this approach when doing cleanup, so we can decide if it is enough or not.
  override final def lookupMaximumLedgerTime(
      ids: Set[ContractId]
  )(implicit connection: Connection): Try[Option[Instant]] = {
    def lookup(id: ContractId): Option[Option[Instant]] =
      SQL"""
  WITH archival_event AS (
         SELECT participant_events.*
           FROM participant_events, parameters
          WHERE contract_id = $id
            AND event_kind = 20  -- consuming exercise
            AND event_sequential_id <= parameters.ledger_end_sequential_id
          LIMIT 1
       ),
       create_event AS (
         SELECT ledger_effective_time
           FROM participant_events, parameters
          WHERE contract_id = $id
            AND event_kind = 10  -- create
            AND event_sequential_id <= parameters.ledger_end_sequential_id
          LIMIT 1 -- limit here to guide planner wrt expected number of results
       ),
       divulged_contract AS (
         SELECT ledger_effective_time
           FROM participant_events, parameters
          WHERE contract_id = $id
            AND event_kind = 0 -- divulgence
            AND event_sequential_id <= parameters.ledger_end_sequential_id
          ORDER BY event_sequential_id
            -- prudent engineering: make results more stable by preferring earlier divulgence events
            -- Results might still change due to pruning.
          LIMIT 1
       ),
       create_and_divulged_contracts AS (
         (SELECT * FROM create_event)   -- prefer create over divulgance events
         UNION ALL
         (SELECT * FROM divulged_contract)
       )
  SELECT ledger_effective_time
    FROM create_and_divulged_contracts
   WHERE NOT EXISTS (SELECT 1 FROM archival_event)
   LIMIT 1;
               """.as(instant("ledger_effective_time").?.singleOpt)

    val queriedIds: List[(ContractId, Option[Option[Instant]])] = ids.toList
      .map(id => id -> lookup(id))
    val foundLedgerEffectiveTimes: List[Option[Instant]] = queriedIds
      .collect { case (_, Some(found)) =>
        found
      }
    if (foundLedgerEffectiveTimes.size != ids.size) {
      val missingIds = queriedIds.collect { case (missingId, None) =>
        missingId
      }
      Failure(MissingContracts(missingIds.toSet))
    } else Success(foundLedgerEffectiveTimes.max)
  }

  override final def lookupParties(parties: Seq[Party])(implicit
      connection: Connection
  ): List[PartyDetails] =
    JdbcLedgerDao.selectParties(parties).map(JdbcLedgerDao.constructPartyDetails)
}
