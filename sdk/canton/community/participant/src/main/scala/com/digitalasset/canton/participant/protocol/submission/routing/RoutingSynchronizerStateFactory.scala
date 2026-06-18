// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.syntax.foldable.*
import com.digitalasset.canton.crypto.SynchronizerCryptoPureApi
import com.digitalasset.canton.error.TransactionRoutingError.{
  UnableToGetStaticParameters,
  UnableToQueryTopologySnapshot,
}
import com.digitalasset.canton.ledger.participant.state.RoutingSynchronizerState
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.submission.routing.RoutingSynchronizerStateFactory.SyncCryptoPureApiLookup
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizer,
  ConnectedSynchronizersLookup,
}
import com.digitalasset.canton.protocol.{LfContractId, StaticSynchronizerParameters}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.client.TopologySnapshotLoader
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

object RoutingSynchronizerStateFactory {
  type SyncCryptoPureApiLookup =
    (PhysicalSynchronizerId, StaticSynchronizerParameters) => Option[SynchronizerCryptoPureApi]

  def create(
      connectedSynchronizers: ConnectedSynchronizersLookup,
      syncCryptoPureApiLookup: SyncCryptoPureApiLookup,
  )(implicit
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
      syncCryptoPureApiLookup = syncCryptoPureApiLookup,
    )
  }
}

class RoutingSynchronizerStateImpl private[routing] (
    val connectedSynchronizers: Map[PhysicalSynchronizerId, ConnectedSynchronizer],
    val topologySnapshots: Map[PhysicalSynchronizerId, TopologySnapshotLoader],
    syncCryptoPureApiLookup: SyncCryptoPureApiLookup,
) extends RoutingSynchronizerState {

  override def getTopologySnapshotFor(
      psid: PhysicalSynchronizerId
  ): Either[UnableToQueryTopologySnapshot.Failed, TopologySnapshotLoader] =
    topologySnapshots
      .get(psid)
      .toRight(UnableToQueryTopologySnapshot.Failed(psid))

  override def getSynchronizersOfContracts(coids: Seq[LfContractId])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[LfContractId, (PhysicalSynchronizerId, ContractStateStatus)]] = {
    type Acc = (Seq[LfContractId], Map[LfContractId, (PhysicalSynchronizerId, ContractStateStatus)])
    connectedSynchronizers
      .collect {
        // only look at synchronizers that are ready for submission
        case (_, connectedSynchronizer: ConnectedSynchronizer)
            if connectedSynchronizer.readyForSubmission.unwrap =>
          connectedSynchronizer
      }
      .toList
      .foldM[FutureUnlessShutdown, Acc](
        (coids, Map.empty[LfContractId, (PhysicalSynchronizerId, ContractStateStatus)]): Acc
      ) {
        // if there are no more cids for which we don't know the synchronizer, we are done
        case ((pending, acc), _) if pending.isEmpty => FutureUnlessShutdown.pure((pending, acc))
        case ((pending, acc), connectedSynchronizer) =>
          // grab the approximate state and check if the contract is currently active on the given synchronizer
          connectedSynchronizer.ephemeral.requestTracker
            .getApproximateStates(pending)
            .map { res =>
              val done =
                acc ++ res.collect {
                  case (cid, status) if status.status.isActive =>
                    (
                      cid,
                      (connectedSynchronizer.psid, ContractStateStatus.Active),
                    )
                  case (cid, status) if status.status.isArchived =>
                    (
                      cid,
                      (connectedSynchronizer.psid, ContractStateStatus.Archived),
                    )
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

  def getSyncCryptoPureApi(
      synchronizerId: PhysicalSynchronizerId
  ): Either[UnableToGetStaticParameters.Failed, Option[SynchronizerCryptoPureApi]] =
    connectedSynchronizers
      .get(synchronizerId)
      .toRight(UnableToGetStaticParameters.Failed(synchronizerId))
      .map { connectedSynchronizer =>
        syncCryptoPureApiLookup(synchronizerId, connectedSynchronizer.staticSynchronizerParameters)
      }
}
