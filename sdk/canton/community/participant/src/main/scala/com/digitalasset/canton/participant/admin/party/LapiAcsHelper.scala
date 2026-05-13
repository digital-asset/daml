// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.participant.admin.data.ActiveContract as ActiveContractValueClass
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

private[participant] object LapiAcsHelper {

  /** Helper to build an ACS source from the Ledger API index service shared by online and offline
    * party replication.
    *
    * @param indexService
    *   ledger api index service exposing ACS querying
    * @param parties
    *   the parties for which to query the ACS
    * @param atOffset
    *   the ledger api offset at which to query the ACS
    * @param excludedStakeholders
    *   set of stakeholders which, if present on a contract, will exclude the contract from the ACS
    *   source
    * @param synchronizerId
    *   optional filter for synchronizer id
    * @param contractSynchronizerRenames
    *   optional map of synchronizer id renames to apply to the returned contracts (used by OffPR)
    * @return
    *   the ACS pekko source of active contracts matching the criteria
    */
  def ledgerApiAcsSource(
      indexService: InternalIndexService,
      parties: Set[PartyId],
      atOffset: Offset,
      excludedStakeholders: Set[PartyId],
      synchronizerId: Option[SynchronizerId],
      contractSynchronizerRenames: Map[String, String] = Map.empty,
  )(implicit traceContext: TraceContext): Source[ActiveContractValueClass, NotUsed] =
    indexService
      .activeContracts(parties.map(_.toLf), Some(atOffset))
      .map(response => response.getActiveContract)
      .filter(contract =>
        synchronizerId
          .forall(filterId => contract.synchronizerId == filterId.toProtoPrimitive)
      )
      .filter { contract =>
        val event = contract.getCreatedEvent
        val stakeholders = (event.signatories ++ event.observers).toSet
        val excludeStakeholdersS = excludedStakeholders.map(_.toProtoPrimitive)
        excludeStakeholdersS.intersect(stakeholders).isEmpty
      }
      .map { contract =>
        if (contractSynchronizerRenames.contains(contract.synchronizerId)) {
          val synchronizerId = contractSynchronizerRenames
            .getOrElse(contract.synchronizerId, contract.synchronizerId)
          contract.copy(synchronizerId = synchronizerId)
        } else {
          contract
        }
      }
      .map(ActiveContractValueClass.tryCreate)
}
