// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.error.TransactionRoutingError.UnableToQueryTopologySnapshot
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** Provides state information about a synchronizer. */
trait RoutingSynchronizerState {

  val topologySnapshots: Map[SynchronizerId, TopologySnapshot]

  /** @return
    *   Right containing the topology snapshot and protocol version for the given
    *   ``synchronizerId``, or Left with an error if the requested synchronizer is not connected
    */
  def getTopologySnapshotAndPVFor(
      synchronizerId: SynchronizerId
  ): Either[UnableToQueryTopologySnapshot.Failed, (TopologySnapshot, ProtocolVersion)]

  def getTopologySnapshotFor(
      synchronizerId: SynchronizerId
  ): Either[UnableToQueryTopologySnapshot.Failed, TopologySnapshot] =
    getTopologySnapshotAndPVFor(synchronizerId).map(_._1)

  def getTopologySnapshotFor(
      synchronizerId: Target[SynchronizerId]
  ): Either[UnableToQueryTopologySnapshot.Failed, Target[TopologySnapshot]] =
    getTopologySnapshotAndPVFor(synchronizerId.unwrap).map(_._1).map(Target(_))

  def getSynchronizersOfContracts(
      coids: Seq[LfContractId]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[LfContractId, SynchronizerId]]
}
