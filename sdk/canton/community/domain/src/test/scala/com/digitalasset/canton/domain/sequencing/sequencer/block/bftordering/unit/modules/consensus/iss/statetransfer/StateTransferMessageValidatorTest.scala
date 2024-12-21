// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferMessageValidator
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.BlockTransferRequest
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
          BlockTransferRequest.create(EpochNumber.First, GenesisEpochNumber, otherSequencerId),
          aMembershipWithOnlySelf,
          Left(
            s"peer $otherSequencerId is requesting state transfer while not being active, active peers are: List($mySequencerId)"
          ),
        ),
        // Genesis start epoch
        (
          BlockTransferRequest.create(GenesisEpochNumber, GenesisEpochNumber, otherSequencerId),
          aMembershipWith2Nodes,
          Left("state transfer is supported only after genesis, but start epoch -1 received"),
        ),
        // Too old start epoch
        (
          BlockTransferRequest.create(EpochNumber.First, EpochNumber.First, otherSequencerId),
          aMembershipWith2Nodes,
          Left("start epoch 0 is not greater than latest completed epoch 0"),
        ),
        // Correct
        (
          BlockTransferRequest.create(EpochNumber(1L), EpochNumber.First, otherSequencerId),
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

  private val mySequencerId = fakeSequencerId("self")
  private val otherSequencerId = fakeSequencerId("other")
  private val aMembershipWithOnlySelf = Membership(mySequencerId)
  private val aMembershipWith2Nodes = Membership(mySequencerId, Set(otherSequencerId))
}
