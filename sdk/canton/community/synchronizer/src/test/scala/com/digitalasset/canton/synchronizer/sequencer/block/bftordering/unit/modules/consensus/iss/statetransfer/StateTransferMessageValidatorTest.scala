// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferMessageValidator
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.BlockTransferRequest
import org.scalatest.wordspec.AnyWordSpec

class StateTransferMessageValidatorTest extends AnyWordSpec with BftSequencerBaseTest {

  import StateTransferMessageValidatorTest.*

  "validate block transfer request" in {
    forAll(
      Table[BlockTransferRequest, Membership, Either[String, Unit]](
        (
          "request",
          "membership",
          "expected result",
        ),
        // Not part of the membership
        (
          BlockTransferRequest.create(EpochNumber.First, otherId),
          aMembershipWithOnlySelf,
          Left(
            s"'$otherId' is requesting state transfer while not being active, active nodes are: List($myId)"
          ),
        ),
        // Genesis start epoch
        (
          BlockTransferRequest.create(GenesisEpochNumber, otherId),
          aMembershipWith2Nodes,
          Left("state transfer is supported only after genesis, but start epoch -1 received"),
        ),
        // Correct
        (
          BlockTransferRequest.create(EpochNumber(1L), otherId),
          aMembershipWith2Nodes,
          Right(()),
        ),
      )
    ) { (request, membership, expectedResult) =>
      StateTransferMessageValidator.validateBlockTransferRequest(
        request,
        membership,
      ) shouldBe expectedResult
    }
  }
}

object StateTransferMessageValidatorTest {

  private val myId = BftNodeId("self")
  private val otherId = BftNodeId("other")
  private val aMembershipWithOnlySelf = Membership.forTesting(myId)
  private val aMembershipWith2Nodes = Membership.forTesting(myId, Set(otherId))
}
