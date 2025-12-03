// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.console.SequencerReference
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  PeerConnectionStatus,
  PeerEndpointHealthStatus,
}

trait CountsAuthenticatedBftPeers {

  /** Helper to count the number of authenticated BFT orderer peers from the perspective of the
    * specified sequencer node.
    */
  protected def numberAuthenticatedPeers(sequencer: SequencerReference): Int =
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
}
