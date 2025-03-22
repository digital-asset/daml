// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferMessageValidator
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.{
  BlockTransferRequest,
  BlockTransferResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer.StateTransferTestHelpers.*
import org.scalatest.wordspec.AnyWordSpec

class StateTransferMessageValidatorTest extends AnyWordSpec with BftSequencerBaseTest {

  import StateTransferMessageValidatorTest.*

  private val validator = new StateTransferMessageValidator(loggerFactory)

  "validate block transfer request" in {
    Table[BlockTransferRequest, Membership, Either[String, Unit]](
      ("request", "membership", "expected result"),
      // negative: not part of the membership
      (
        BlockTransferRequest.create(EpochNumber.First, otherId),
        aMembershipWithOnlySelf,
        Left(
          s"'$otherId' is requesting state transfer while not being active, active nodes are: List($myId)"
        ),
      ),
      // negative: genesis start epoch
      (
        BlockTransferRequest.create(GenesisEpochNumber, otherId),
        aMembershipWith2Nodes,
        Left("state transfer is supported only after genesis, but start epoch -1 received"),
      ),
      // positive
      (
        BlockTransferRequest.create(EpochNumber(1L), otherId),
        aMembershipWith2Nodes,
        Right(()),
      ),
    ).forEvery { (request, membership, expectedResult) =>
      validator.validateBlockTransferRequest(
        request,
        membership,
      ) shouldBe expectedResult
    }
  }

  "validate block transfer response" in {
    Table(
      (
        "block transfer response",
        "latest locally completed epoch",
        "membership",
        "expected result",
      ),
      // negative: inactive node
      (
        BlockTransferResponse.create(None, EpochNumber.First, otherId),
        EpochNumber.First,
        aMembershipWithOnlySelf,
        Left(
          "received a block transfer response from 'other' which has not been active, active nodes: List(self)"
        ),
      ),
      // negative: unexpected epoch in pre-prepare
      (
        BlockTransferResponse.create(
          Some(
            aCommitCert().copy(prePrepare =
              aPrePrepare(BlockMetadata(GenesisEpochNumber, BlockNumber.First))
            )
          ),
          EpochNumber.First,
          otherId,
        ),
        EpochNumber.First,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' containing a pre-prepare with unexpected epoch Some(-1), " +
            "expected 1"
        ),
      ),
      // negative: unexpected epoch in commits
      (
        BlockTransferResponse.create(
          Some(
            aCommitCert(BlockMetadata(EpochNumber.First, BlockNumber.First))
              .copy(prePrepare = aPrePrepare(BlockMetadata(EpochNumber(1L), BlockNumber.First)))
          ),
          EpochNumber.First,
          otherId,
        ),
        EpochNumber.First,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' containing commit(s) with an unexpected epoch, expected 1"
        ),
      ),
      // negative: duplicate senders
      (
        BlockTransferResponse.create(
          Some(aCommitCert().copy(commits = Seq(aCommit(), aCommit()))),
          EpochNumber.First,
          otherId,
        ),
        GenesisEpochNumber,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' containing commits with duplicate senders"
        ),
      ),
      // negative: no strong quorum
      (
        BlockTransferResponse.create(
          Some(aCommitCert().copy(commits = Seq(aCommit()))),
          EpochNumber.First,
          otherId,
        ),
        GenesisEpochNumber,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' with insufficient number of commits Some(1), " +
            "the minimal number is 2 (strong quorum)"
        ),
      ),
      // negative: invalid latest completed epoch
      (
        BlockTransferResponse.create(None, EpochNumber(-1500), otherId),
        GenesisEpochNumber,
        aMembershipWithOnlyOtherNode,
        Left(
          "received a block transfer response from 'other' with invalid latest completed epoch -1500"
        ),
      ),
      // positive
      (
        BlockTransferResponse.create(Some(aCommitCert()), EpochNumber.First, otherId),
        GenesisEpochNumber,
        aMembershipWithOnlyOtherNode,
        Right(()),
      ),
    ).forEvery { (response, latestLocallyCompletedEpoch, membership, expectedResult) =>
      validator.validateBlockTransferResponse(
        response,
        latestLocallyCompletedEpoch,
        membership,
      ) shouldBe expectedResult
    }
  }
}

object StateTransferMessageValidatorTest {
  private val aMembershipWithOnlySelf = Membership.forTesting(myId)
  private val aMembershipWithOnlyOtherNode =
    Membership(
      myId,
      OrderingTopology.forTesting(Set(otherId), SequencingParameters.Default),
      Seq(otherId),
    )
  private val aMembershipWith2Nodes = Membership.forTesting(myId, Set(otherId))
}
