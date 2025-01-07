// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.syntax.foldable.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.sync.TransactionRoutingError.UnableToQueryTopologySnapshot
import com.digitalasset.canton.participant.sync.{ConnectedDomainsLookup, SyncDomain}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** Provides state information about a domain. */
trait DomainStateProvider {

  /** Returns an either rather than an option since failure comes from disconnected
    * domains and we assume the participant to be connected to all domains in `connectedDomains`
    */
  def getTopologySnapshotAndPVFor(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): Either[UnableToQueryTopologySnapshot.Failed, (TopologySnapshot, ProtocolVersion)]

  def getTopologySnapshotFor(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): Either[UnableToQueryTopologySnapshot.Failed, TopologySnapshot] =
    getTopologySnapshotAndPVFor(synchronizerId).map(_._1)

  def getTopologySnapshotFor(synchronizerId: Target[SynchronizerId])(implicit
      traceContext: TraceContext
  ): Either[UnableToQueryTopologySnapshot.Failed, Target[TopologySnapshot]] =
    getTopologySnapshotAndPVFor(synchronizerId.unwrap).map(_._1).map(Target(_))

  def getDomainsOfContracts(
      coids: Seq[LfContractId]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[LfContractId, SynchronizerId]]

}

class DomainStateProviderImpl(connectedDomains: ConnectedDomainsLookup)
    extends DomainStateProvider {
  override def getTopologySnapshotAndPVFor(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): Either[UnableToQueryTopologySnapshot.Failed, (TopologySnapshot, ProtocolVersion)] =
    connectedDomains
      .get(synchronizerId)
      .toRight(UnableToQueryTopologySnapshot.Failed(synchronizerId))
      .map { syncDomain =>
        (
          syncDomain.topologyClient.currentSnapshotApproximation,
          syncDomain.staticSynchronizerParameters.protocolVersion,
        )
      }

  override def getDomainsOfContracts(coids: Seq[LfContractId])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[LfContractId, SynchronizerId]] = {
    type Acc = (Seq[LfContractId], Map[LfContractId, SynchronizerId])
    connectedDomains.snapshot
      .collect {
        // only look at domains that are ready for submission
        case (_, syncDomain: SyncDomain) if syncDomain.readyForSubmission.unwrap => syncDomain
      }
      .toList
      .foldM[FutureUnlessShutdown, Acc]((coids, Map.empty[LfContractId, SynchronizerId]): Acc) {
        // if there are no more cids for which we don't know the domain, we are done
        case ((pending, acc), _) if pending.isEmpty => FutureUnlessShutdown.pure((pending, acc))
        case ((pending, acc), syncDomain) =>
          // grab the approximate state and check if the contract is currently active on the given domain
          syncDomain.ephemeral.requestTracker
            .getApproximateStates(pending)
            .map { res =>
              val done = acc ++ res.collect {
                case (cid, status) if status.status.isActive => (cid, syncDomain.synchronizerId)
              }
              (pending.filterNot(cid => done.contains(cid)), done)
            }
      }
      .map(_._2)
  }
}
