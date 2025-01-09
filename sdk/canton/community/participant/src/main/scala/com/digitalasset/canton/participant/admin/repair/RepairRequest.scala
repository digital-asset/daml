// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.ProcessingStartingPoints
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{StaticSynchronizerParameters, TransactionId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.{RequestCounter, SynchronizerAlias}

private[repair] final case class RepairRequest(
    synchronizer: RepairRequest.SynchronizerData,
    transactionId: TransactionId,
    requestCounters: NonEmpty[Seq[RequestCounter]],
    context: RepairContext,
) {

  val timestamp: CantonTimestamp = synchronizer.startingPoints.processing.currentRecordTime

  def firstTimeOfChange: TimeOfChange = TimeOfChange(requestCounters.head1, timestamp)

  def timesOfChange: Seq[TimeOfChange] =
    requestCounters.map(rc => TimeOfChange(rc, timestamp))

  def requestData: Seq[RequestData] =
    // Trace context persisted explicitly doubling as a marker for repair requests in the request journal
    requestCounters.map(rc => RequestData(rc, RequestState.Pending, timestamp, Some(context)))

  def tryExactlyOneRequestCounter: RequestCounter =
    if (requestCounters.sizeIs == 1) requestCounters.head1
    else
      throw new RuntimeException(
        s"Expected 1 request counter, actual size is ${requestCounters.size}"
      )

  def tryExactlyOneTimeOfChange: TimeOfChange =
    TimeOfChange(tryExactlyOneRequestCounter, timestamp)

}

private[repair] object RepairRequest {

  final case class SynchronizerData(
      id: SynchronizerId,
      alias: SynchronizerAlias,
      topologySnapshot: TopologySnapshot,
      persistentState: SyncDomainPersistentState,
      parameters: StaticSynchronizerParameters,
      startingPoints: ProcessingStartingPoints,
  )

}
