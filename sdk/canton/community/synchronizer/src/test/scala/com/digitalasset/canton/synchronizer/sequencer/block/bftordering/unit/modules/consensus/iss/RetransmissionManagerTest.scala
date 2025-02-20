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
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
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

  val segmentStatus0 = Consensus.RetransmissionsMessage.SegmentStatus(
    EpochNumber.First,
    segmentIndex = 0,
    ConsensusStatus.SegmentStatus.Complete,
  )
  val segmentStatus1 = Consensus.RetransmissionsMessage.SegmentStatus(
    EpochNumber.First,
    segmentIndex = 1,
    ConsensusStatus.SegmentStatus.Complete,
  )

  val retransmissionRequest =
    Consensus.RetransmissionsMessage.RetransmissionRequest.create(
      ConsensusStatus.EpochStatus(
        self,
        EpochNumber.First,
        Seq(
          ConsensusStatus.SegmentStatus.Complete,
          ConsensusStatus.SegmentStatus.Complete,
        ),
      )
    )

  val epochStatus =
    ConsensusStatus.EpochStatus(
      other1,
      EpochNumber.First,
      Seq(
        ConsensusStatus.SegmentStatus.InProgress(
          ViewNumber.First,
          Seq(ConsensusStatus.BlockStatus.InProgress(false, Seq.empty, Seq.empty)),
        )
      ),
    )

  def verifySentRequestNRetransmissionRequests(
      cryptoProvider: CryptoProvider[ProgrammableUnitTestEnv],
      networkOut: ModuleRef[P2PNetworkOut.Message],
      wantedNumberOfInvocations: Int,
  ): Unit = {
    verify(cryptoProvider, times(wantedNumberOfInvocations)).signMessage(
      retransmissionRequest,
      HashPurpose.BftSignedRetransmissionMessage,
      SigningKeyUsage.ProtocolOnly,
    )
    verify(networkOut, times(wantedNumberOfInvocations)).asyncSend(
      P2PNetworkOut.Multicast(
        P2PNetworkOut.BftOrderingNetworkMessage.RetransmissionMessage(
          retransmissionRequest.fakeSign
        ),
        otherPeers,
      )
    )
  }

  "RetransmissionManager" should {
    "send request upon epoch start" in {
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

      manager.handleMessage(cryptoProvider, segmentStatus0)

      context.runPipedMessages() shouldBe List()
      verifyZeroInteractions(cryptoProvider)

      manager.handleMessage(cryptoProvider, segmentStatus1)

      context.runPipedMessages() shouldBe List()

      verifySentRequestNRetransmissionRequests(
        cryptoProvider,
        networkOut,
        wantedNumberOfInvocations = 1,
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
        logMessage => logMessage.level shouldBe Level.INFO,
      )
    }

    "not send epoch status if epoch ends" in {
      val networkOut = mock[ModuleRef[P2PNetworkOut.Message]]
      implicit val context
          : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext()
      val manager = createManager(networkOut)

      val epochState = mock[EpochState[ProgrammableUnitTestEnv]]

      when(epochState.epoch).thenReturn(epoch)
      when(epochState.requestSegmentStatuses())
        .thenReturn(new EpochStatusBuilder(self, EpochNumber.First, 1 + otherPeers.size))

      val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)

      manager.startEpoch(epochState)

      manager.handleMessage(cryptoProvider, segmentStatus0)

      // since epoch ended, segment status for the current epoch will not be broadcast
      manager.epochEnded(Seq.empty)

      manager.handleMessage(cryptoProvider, segmentStatus1)
      manager.handleMessage(cryptoProvider, segmentStatus0)

      context.runPipedMessages() shouldBe List()
      verifyZeroInteractions(cryptoProvider)
      verifyZeroInteractions(networkOut)
    }

    "schedule new status broadcast after finishing one" in {
      val networkOut = mock[ModuleRef[P2PNetworkOut.Message]]
      implicit val context
          : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext()
      val manager = createManager(networkOut)

      val epochState = mock[EpochState[ProgrammableUnitTestEnv]]

      when(epochState.epoch).thenReturn(epoch)
      def builder = new EpochStatusBuilder(self, EpochNumber.First, 1 + otherPeers.size)
      when(epochState.requestSegmentStatuses())
        .thenReturn(builder, builder) // we'll need 2 fresh builders in this test

      val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)

      manager.startEpoch(epochState)

      context.lastDelayedMessage shouldBe empty

      manager.handleMessage(cryptoProvider, segmentStatus0)
      manager.handleMessage(cryptoProvider, segmentStatus1)
      context.runPipedMessages() shouldBe List()

      verifySentRequestNRetransmissionRequests(
        cryptoProvider,
        networkOut,
        wantedNumberOfInvocations = 1,
      )

      val periodicMsg = Consensus.RetransmissionsMessage.PeriodicStatusBroadcast

      context.lastDelayedMessage should contain((1, periodicMsg))

      manager.handleMessage(cryptoProvider, periodicMsg)

      manager.handleMessage(cryptoProvider, segmentStatus0)

      context.runPipedMessages() shouldBe List()
      verifySentRequestNRetransmissionRequests(
        cryptoProvider,
        networkOut,
        wantedNumberOfInvocations = 1,
      )

      context.lastCancelledEvent shouldBe empty

      manager.handleMessage(cryptoProvider, segmentStatus1)

      context.runPipedMessages() shouldBe List()
      verifySentRequestNRetransmissionRequests(
        cryptoProvider,
        networkOut,
        wantedNumberOfInvocations = 2,
      )

      // after completing broadcasting the request, a new broadcast gets scheduled
      context.lastCancelledEvent.map(_._1) should contain(1)
      context.lastDelayedMessage should contain((2, periodicMsg))

      // ending the epoch cancels the scheduling of the next broadcast
      manager.epochEnded(Seq.empty)
      context.lastCancelledEvent.map(_._1) should contain(2)
    }

    "process retransmission request of same epoch using the epoch state" in {
      implicit val context
          : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext()

      val epochState = mock[EpochState[ProgrammableUnitTestEnv]]
      when(epochState.epoch).thenReturn(epoch)
      doNothing.when(epochState).processRetransmissionsRequest(epochStatus)

      val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)
      val manager = createManager(mock[ModuleRef[P2PNetworkOut.Message]])

      manager.startEpoch(epochState)

      manager.handleMessage(
        cryptoProvider,
        Consensus.RetransmissionsMessage.VerifiedNetworkMessage(
          Consensus.RetransmissionsMessage.RetransmissionRequest.create(epochStatus)
        ),
      )

      verify(epochState, times(1)).processRetransmissionsRequest(epochStatus)
    }

    "process retransmission request using past commit certificates if epoch has ended" in {
      implicit val context
          : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext()

      val epochState = mock[EpochState[ProgrammableUnitTestEnv]]
      when(epochState.epoch).thenReturn(epoch)

      val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)
      val networkOut = mock[ModuleRef[P2PNetworkOut.Message]]
      val manager = createManager(networkOut)

      val commitCertificate = CommitCertificate(
        PrePrepare
          .create(
            BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
            ViewNumber.First,
            CantonTimestamp.Epoch,
            OrderingBlock(Seq()),
            CanonicalCommitSet.empty,
            from = self,
          )
          .fakeSign,
        Seq.empty,
      )

      manager.startEpoch(epochState)
      manager.epochEnded(Seq(commitCertificate))

      manager.handleMessage(
        cryptoProvider,
        Consensus.RetransmissionsMessage.VerifiedNetworkMessage(
          Consensus.RetransmissionsMessage.RetransmissionRequest.create(epochStatus)
        ),
      )

      context.runPipedMessages() shouldBe empty

      val retransmissionResponse =
        Consensus.RetransmissionsMessage.RetransmissionResponse.create(
          self,
          Seq(commitCertificate),
        )

      verify(cryptoProvider, times(1)).signMessage(
        retransmissionResponse,
        HashPurpose.BftSignedRetransmissionMessage,
        SigningKeyUsage.ProtocolOnly,
      )
      verify(networkOut, times(1)).asyncSend(
        P2PNetworkOut.Multicast(
          P2PNetworkOut.BftOrderingNetworkMessage.RetransmissionMessage(
            retransmissionResponse.fakeSign
          ),
          Set(other1),
        )
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
