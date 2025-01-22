// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.BlockTransferRequest

object StateTransferMessageValidator {

  def validateBlockTransferRequest(
      request: BlockTransferRequest,
      activeMembership: Membership,
  ): Either[String, Unit] = {
    val from = request.from
    val peers = activeMembership.sortedPeers

    for {
      _ <- Either.cond(
        peers.contains(from),
        (),
        s"peer $from is requesting state transfer while not being active, active peers are: $peers",
      )
      _ <- Either.cond(
        request.startEpoch > Genesis.GenesisEpochNumber,
        (),
        s"state transfer is supported only after genesis, but start epoch ${request.startEpoch} received",
      )
      _ <- Either.cond(
        request.startEpoch > request.latestCompletedEpoch,
        (),
        s"start epoch ${request.startEpoch} is not greater than latest completed epoch ${request.latestCompletedEpoch}",
      )
    } yield ()
  }
}
