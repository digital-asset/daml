// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss

import com.digitalasset.canton.crypto.{HashPurpose, SignatureCheckError, SigningKeyUsage}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.leaders.SimpleLeaderSelectionPolicy
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions.{
  EpochStatusBuilder,
  RetransmissionsManager,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusStatus,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.{
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

class RetransmissionManagerTest extends AnyWordSpec with BftSequencerBaseTest {
  val self = fakeSequencerId("self")
  val other1 = fakeSequencerId("other1")
  val otherPeers = Set(other1)

  val membership = Membership.apply(self, otherPeers)
  val leaderSelectionPolicy = SimpleLeaderSelectionPolicy
  val epoch = EpochState.Epoch(
    EpochInfo(
      EpochNumber.First,
      BlockNumber.First,
      EpochLength(10),
      TopologyActivationTime(CantonTimestamp.Epoch),
    ),
    membership,
    membership,
    leaderSelectionPolicy,
  )

  "RetransmissionManager" should {
    "Send request upon epoch start" in {
      val networkOut = mock[ModuleRef[P2PNetworkOut.Message]]
      implicit val context
          : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext()
      val manager = createManager(networkOut)

      val epochState = mock[EpochState[ProgrammableUnitTestEnv]]

      when(epochState.epoch).thenReturn(epoch)
      when(epochState.requestSegmentStatuses())
        .thenReturn(new EpochStatusBuilder(self, EpochNumber.First, 1 + otherPeers.size))

      manager.startEpoch(epochState)

      val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)

      val segmentStatus0 = Consensus.RetransmissionsMessage.SegmentStatus(
        EpochNumber.First,
        segmentIndex = 0,
        ConsensusStatus.SegmentStatus.Complete,
      )
      manager.handleMessage(cryptoProvider, segmentStatus0)

      context.runPipedMessages() shouldBe List()
      verifyZeroInteractions(cryptoProvider)

      val segmentStatus1 = Consensus.RetransmissionsMessage.SegmentStatus(
        EpochNumber.First,
        segmentIndex = 1,
        ConsensusStatus.SegmentStatus.Complete,
      )
      manager.handleMessage(cryptoProvider, segmentStatus1)

      context.runPipedMessages() shouldBe List()

      val epochStatus = ConsensusStatus.EpochStatus(
        self,
        EpochNumber.First,
        Seq(
          ConsensusStatus.SegmentStatus.Complete,
          ConsensusStatus.SegmentStatus.Complete,
        ),
      )
      val retransmissionRequest =
        Consensus.RetransmissionsMessage.RetransmissionRequest.create(epochStatus)
      verify(cryptoProvider, times(1)).signMessage(
        retransmissionRequest,
        HashPurpose.BftSignedRetransmissionMessage,
        SigningKeyUsage.ProtocolOnly,
      )
      verify(networkOut, times(1)).asyncSend(
        P2PNetworkOut.Multicast(
          P2PNetworkOut.BftOrderingNetworkMessage.RetransmissionMessage(
            retransmissionRequest.fakeSign
          ),
          otherPeers,
        )
      )
    }

    "verify network messages" when {
      "continue process if verification is successful" in {
        val networkOut = mock[ModuleRef[P2PNetworkOut.Message]]
        implicit val context
            : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]]()
        val manager = createManager(networkOut)

        val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]

        val message = mock[Consensus.RetransmissionsMessage.RetransmissionsNetworkMessage]

        when(
          cryptoProvider.verifySignedMessage(
            message.fakeSign,
            HashPurpose.BftSignedRetransmissionMessage,
            SigningKeyUsage.ProtocolOnly,
          )
        ).thenReturn(() => Right(()))

        manager.handleMessage(
          cryptoProvider,
          Consensus.RetransmissionsMessage.UnverifiedNetworkMessage(message.fakeSign),
        )

        context.runPipedMessages() shouldBe List(
          Consensus.RetransmissionsMessage.VerifiedNetworkMessage(message)
        )
      }
    }

    "drop message if verification failed" in {
      val networkOut = mock[ModuleRef[P2PNetworkOut.Message]]
      implicit val context
          : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]]()
      val manager = createManager(networkOut)

      val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]

      val message = mock[Consensus.RetransmissionsMessage.RetransmissionsNetworkMessage]

      when(
        cryptoProvider.verifySignedMessage(
          message.fakeSign,
          HashPurpose.BftSignedRetransmissionMessage,
          SigningKeyUsage.ProtocolOnly,
        )
      ).thenReturn(() => Left(SignatureCheckError.InvalidKeyError("failed to verify")))

      manager.handleMessage(
        cryptoProvider,
        Consensus.RetransmissionsMessage.UnverifiedNetworkMessage(message.fakeSign),
      )

      assertLogs(
        context.runPipedMessages() shouldBe List(),
        logMessage => logMessage.level shouldBe Level.WARN,
      )
    }
  }

  private def createManager(
      networkOut: ModuleRef[P2PNetworkOut.Message]
  ): RetransmissionsManager[ProgrammableUnitTestEnv] =
    new RetransmissionsManager[ProgrammableUnitTestEnv](
      self,
      networkOut,
      fail(_),
      previousEpochsCommitCerts = Map.empty,
      loggerFactory,
    )
}
