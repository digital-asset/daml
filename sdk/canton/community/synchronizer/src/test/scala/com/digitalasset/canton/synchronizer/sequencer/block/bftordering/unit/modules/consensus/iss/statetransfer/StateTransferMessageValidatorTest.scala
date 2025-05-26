// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.{
  BlockTransferRequest,
  BlockTransferResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer.StateTransferTestHelpers.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.{
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
  failingCryptoProvider,
}
import org.scalatest.wordspec.AnyWordSpec

class StateTransferMessageValidatorTest extends AnyWordSpec with BftSequencerBaseTest {

  import StateTransferMessageValidatorTest.*

  private val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
  private val validator =
    new StateTransferMessageValidator[ProgrammableUnitTestEnv](metrics, loggerFactory)

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
        BlockTransferResponse.create(None, otherId),
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
          otherId,
        ),
        EpochNumber.First,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' containing a commit certificate with the following issue: commit certificate for block 0 has the following errors: commits have epoch number 0 but it should be 1, expected at least 2 commits, but only got 1, commit from other has non-matching hash"
        ),
      ),
      // negative: duplicate senders
      (
        BlockTransferResponse.create(
          Some(aCommitCert().copy(commits = Seq(aCommit(), aCommit()))),
          otherId,
        ),
        GenesisEpochNumber,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' containing a commit certificate with the following issue: commit certificate for block 0 has the following errors: there are more than one commits (2) from the same sender other, expected at least 2 commits, but only got 1, commit from other has non-matching hash"
        ),
      ),
      // negative: no strong quorum
      (
        BlockTransferResponse.create(
          Some(aCommitCert().copy(commits = Seq(aCommit()))),
          otherId,
        ),
        GenesisEpochNumber,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' containing a commit certificate with the following issue: commit certificate for block 0 has the following errors: expected at least 2 commits, but only got 1, commit from other has non-matching hash"
        ),
      ),
      // positive
      (
        BlockTransferResponse.create(Some(aCommitCert()), otherId),
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

  "skip block transfer response signature verification" in {
    implicit val context: ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
      new ProgrammableUnitTestContext

    val response = BlockTransferResponse.create(None, otherId)
    validator.verifyStateTransferMessage(
      response.fakeSign,
      aMembershipWith2Nodes,
      failingCryptoProvider,
    )

    context.extractSelfMessages() should contain only
      Consensus.StateTransferMessage.VerifiedStateTransferMessage(response)
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
