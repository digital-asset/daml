// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import com.digitalasset.canton.crypto.SignatureCheckError.SignerHasNoValidKeys
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Prepare
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.{
  FailingCryptoProvider,
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import org.scalatest.wordspec.AnyWordSpec

class IssConsensusSignatureVerifierTest extends AnyWordSpec with BftSequencerBaseTest {
  val myId: BftNodeId = BftNodeId("self")
  val otherIds: IndexedSeq[BftNodeId] = (1 to 3).map(index => BftNodeId(s"node$index"))
  val allIds: Seq[BftNodeId] = (myId +: otherIds).sorted
  val anOrderingTopology = OrderingTopology.forTesting(allIds.toSet)
  val aMembership = Membership(myId, anOrderingTopology, allIds)
  def aTopologyInfo(
      currentCryptoProvider: CryptoProvider[ProgrammableUnitTestEnv] = new FailingCryptoProvider()
  ) =
    OrderingTopologyInfo[ProgrammableUnitTestEnv](
      myId,
      anOrderingTopology,
      currentCryptoProvider,
      allIds,
      previousTopology = anOrderingTopology, // not relevant
      currentCryptoProvider,
      allIds,
    )

  val blockMetadata = BlockMetadata(EpochNumber.First, BlockNumber.First)

  "IssConsensusSignatureVerifier" should {
    "verify signature of message" in {
      implicit val context: ProgrammableUnitTestContext[Prepare] = new ProgrammableUnitTestContext
      val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)

      val verifier = new IssConsensusSignatureVerifier[ProgrammableUnitTestEnv](
        SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
      )
      val hash = TestHash.digest("pre-prepare")
      val prepare = Prepare
        .create(
          blockMetadata,
          ViewNumber.First,
          hash,
          myId,
        )
        .fakeSign
      val result = verifier.verify(prepare, aTopologyInfo(cryptoProvider))

      verify(cryptoProvider, times(1)).verifySignedMessage(
        prepare,
        AuthenticatedMessageType.BftSignedConsensusMessage,
      )

      result() shouldBe Right(())
    }

    "check if the node is a member before checking signature" in {
      implicit val context: ProgrammableUnitTestContext[Prepare] = new ProgrammableUnitTestContext
      val verifier = new IssConsensusSignatureVerifier[ProgrammableUnitTestEnv](
        SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
      )
      val prepare = Prepare
        .create(
          blockMetadata,
          ViewNumber.First,
          TestHash.digest("pre-prepare"),
          BftNodeId("InvalidNode"),
        )
        .fakeSign
      val result = verifier.verify(prepare, aTopologyInfo())
      result() shouldBe Left(
        List(
          SignerHasNoValidKeys(
            "Cannot verify signature from node InvalidNode, because it is not currently a valid member"
          )
        )
      )
    }
  }
}
