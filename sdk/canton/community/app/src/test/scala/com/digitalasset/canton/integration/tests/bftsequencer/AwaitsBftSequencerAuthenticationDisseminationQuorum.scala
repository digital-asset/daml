// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsequencer

import com.digitalasset.canton.console.SequencerReference
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  PeerConnectionStatus,
  PeerEndpointHealthStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.{BaseTest, integration}
import org.scalatest.Assertion

trait AwaitsBftSequencerAuthenticationDisseminationQuorum {
  this: BaseTest =>

  /** Helper to count the number of authenticated BFT orderer peers from the perspective of the
    * specified sequencer node.
    */
  private def authenticatedPeersCount(sequencer: SequencerReference): Int =
    sequencer.bft
      .get_peer_network_status(None)
      .endpointStatuses
      .count {
        case PeerConnectionStatus.PeerEndpointStatus(_, _, health) =>
          health.status match {
            case PeerEndpointHealthStatus.Authenticated(_) => true
            case _ => false
          }
        // Incoming connections as counted as authenticated as they have gotten past the
        // sequencer connection authentication challenge:
        case PeerConnectionStatus.PeerIncomingConnection(_) => true
      }

  /** Helper to wait until all sequencers have authenticated at least a weak quorum of BFT peers
    * (i.e., a quorum that allows the dissemination sub-protocol to run and thus prevents rejection
    * of submission requests).
    *
    * Useful upon test initialization to prevent flaky test log noise such as
    *   - "P2P connectivity is not ready (authenticated = 1 < dissemination quorum = 2)"
    */
  protected def waitUntilAllBftSequencersAuthenticateDisseminationQuorum()(implicit
      env: integration.TestConsoleEnvironment
  ): Assertion = {
    val weakQuorumSize = OrderingTopology.weakQuorumSize(env.sequencers.all.size)
    clue(
      s"make sure all sequencers have connected to a dissemination quorum of at least $weakQuorumSize other sequencers"
    ) {
      eventually() {
        forAll(env.sequencers.all)(authenticatedPeersCount(_) should be >= weakQuorumSize)
      }
    }
  }
}
