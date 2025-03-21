// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.protocol.{StaticSynchronizerParameters, TransactionId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.{RepairCounter, SynchronizerAlias}

private[repair] final case class RepairRequest(
    synchronizer: RepairRequest.SynchronizerData,
    transactionId: TransactionId,
    repairCounters: NonEmpty[Seq[RepairCounter]],
) {

  val timestamp: CantonTimestamp = synchronizer.currentRecordTime

  def firstTimeOfRepair: TimeOfRepair = TimeOfRepair(timestamp, repairCounters.head1)

  def timesOfRepair: Seq[TimeOfRepair] =
    repairCounters.map(repairCounter => TimeOfRepair(timestamp, repairCounter))

  def tryExactlyOneRepairCounter: RepairCounter =
    if (repairCounters.sizeIs == 1) repairCounters.head1
    else
      throw new RuntimeException(
        s"Expected 1 repair counter, actual size is ${repairCounters.size}"
      )

  def tryExactlyOneTimeOfRepair: TimeOfRepair =
    TimeOfRepair(timestamp, tryExactlyOneRepairCounter)

}

private[repair] object RepairRequest {

  /** @param synchronizerIndex
    *   Should be provided as it returned from the ledgerApiStore (required to provide,
    *   synchronizerIndex is optional in nature)
    */
  final case class SynchronizerData(
      id: SynchronizerId,
      alias: SynchronizerAlias,
      topologySnapshot: TopologySnapshot,
      persistentState: SyncPersistentState,
      parameters: StaticSynchronizerParameters,
      currentRecordTime: CantonTimestamp,
      nextRepairCounter: RepairCounter,
      synchronizerIndex: Option[SynchronizerIndex],
  )

}
