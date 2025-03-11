// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.syntax.foldable.*
import com.digitalasset.canton.error.TransactionRoutingError.UnableToQueryTopologySnapshot
import com.digitalasset.canton.ledger.participant.state.RoutingSynchronizerState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizer,
  ConnectedSynchronizersLookup,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.TopologySnapshotLoader
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

object RoutingSynchronizerStateFactory {
  def create(connectedSynchronizers: ConnectedSynchronizersLookup)(implicit
      traceContext: TraceContext
  ): RoutingSynchronizerStateImpl = {
    val connectedSynchronizersSnapshot = connectedSynchronizers.snapshot.toMap

    // Ensure we have a constant snapshot for the lifetime of this object.
    val topologySnapshots = connectedSynchronizersSnapshot.view.mapValues {
      _.topologyClient.currentSnapshotApproximation
    }.toMap

    new RoutingSynchronizerStateImpl(
      connectedSynchronizers = connectedSynchronizersSnapshot,
      topologySnapshots = topologySnapshots,
    )
  }
}

class RoutingSynchronizerStateImpl private[routing] (
    connectedSynchronizers: Map[SynchronizerId, ConnectedSynchronizer],
    val topologySnapshots: Map[SynchronizerId, TopologySnapshotLoader],
) extends RoutingSynchronizerState {

  override def getTopologySnapshotAndPVFor(
      synchronizerId: SynchronizerId
  ): Either[UnableToQueryTopologySnapshot.Failed, (TopologySnapshotLoader, ProtocolVersion)] =
    connectedSynchronizers
      .get(synchronizerId)
      .toRight(UnableToQueryTopologySnapshot.Failed(synchronizerId))
      .map { connectedSynchronizer =>
        (
          topologySnapshots(synchronizerId),
          connectedSynchronizer.staticSynchronizerParameters.protocolVersion,
        )
      }

  override def getSynchronizersOfContracts(coids: Seq[LfContractId])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[LfContractId, SynchronizerId]] = {
    type Acc = (Seq[LfContractId], Map[LfContractId, SynchronizerId])
    connectedSynchronizers
      .collect {
        // only look at synchronizers that are ready for submission
        case (_, connectedSynchronizer: ConnectedSynchronizer)
            if connectedSynchronizer.readyForSubmission.unwrap =>
          connectedSynchronizer
      }
      .toList
      .foldM[FutureUnlessShutdown, Acc]((coids, Map.empty[LfContractId, SynchronizerId]): Acc) {
        // if there are no more cids for which we don't know the synchronizer, we are done
        case ((pending, acc), _) if pending.isEmpty => FutureUnlessShutdown.pure((pending, acc))
        case ((pending, acc), connectedSynchronizer) =>
          // grab the approximate state and check if the contract is currently active on the given synchronizer
          connectedSynchronizer.ephemeral.requestTracker
            .getApproximateStates(pending)
            .map { res =>
              val done = acc ++ res.collect {
                case (cid, status) if status.status.isActive =>
                  (cid, connectedSynchronizer.synchronizerId)
              }
              (pending.filterNot(cid => done.contains(cid)), done)
            }
      }
      .map(_._2)
  }

  override def existsReadySynchronizer(): Boolean =
    connectedSynchronizers.view.exists { case (_syncId, sync) =>
      sync.ready
    }
}
