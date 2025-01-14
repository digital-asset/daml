// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultLeaderSelectionPolicy
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  Genesis,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.{
  StateTransferManager,
  StateTransferMessageResult,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.NetworkMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss.InMemoryUnitTestEpochStore
import com.digitalasset.canton.topology.SequencerId
import org.scalatest.wordspec.AnyWordSpec

class StateTransferManagerTest extends AnyWordSpec with BftSequencerBaseTest {
  import StateTransferManagerTest.*

  "StateTransferManager" should {
    "start and try to restart" in {
      implicit val context: ContextType = new ProgrammableUnitTestContext

      val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef
        )

      stateTransferManager.inStateTransfer shouldBe false

      val startEpoch = EpochNumber(7L)
      stateTransferManager.startStateTransfer(
        membership,
        latestCompletedEpoch = Genesis.GenesisEpoch,
        startEpoch,
      )(abort = fail(_))

      stateTransferManager.inStateTransfer shouldBe true

      val blockTransferRequest = StateTransferMessage.BlockTransferRequest
        .create(
          startEpoch,
          latestCompletedEpoch = Genesis.GenesisEpochNumber,
          from = mySequencerId,
        )
        .fakeSign
      assertBlockTransferRequestHasBeenSent(
        p2pNetworkOutRef,
        blockTransferRequest,
        to = otherSequencerId,
        numberOfTimes = 1,
      )

      // Try to start state transfer (with no effect) while another one is in progress.
      stateTransferManager.startStateTransfer(
        membership,
        latestCompletedEpoch = Genesis.GenesisEpoch,
        startEpoch,
      )(abort = fail(_))
      context.extractSelfMessages() shouldBe empty
    }

    "schedule a retry and send block transfer request" in {
      implicit val context: ContextType = new ProgrammableUnitTestContext()

      val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef
        )

      // Initiate state transfer so that it's in progress.
      val latestCompletedEpoch = Genesis.GenesisEpoch
      val startEpoch = EpochNumber(7L)
      stateTransferManager.startStateTransfer(
        membership,
        latestCompletedEpoch,
        startEpoch,
      )(abort = fail(_))

      val blockTransferRequest = StateTransferMessage.BlockTransferRequest
        .create(
          startEpoch,
          latestCompletedEpoch = Genesis.GenesisEpochNumber,
          from = mySequencerId,
        )
        .fakeSign
      stateTransferManager.handleStateTransferMessage(
        StateTransferMessage
          .ResendBlockTransferRequest(blockTransferRequest, to = otherSequencerId),
        membership,
        latestCompletedEpoch,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

      assertBlockTransferRequestHasBeenSent(
        p2pNetworkOutRef,
        blockTransferRequest,
        to = otherSequencerId,
        numberOfTimes = 2, // +1 from start
      )
    }

    "send an empty block transfer response on a request for onboarding when the serving node is lagging behind" in {
      implicit val context: ContextType = new ProgrammableUnitTestContext()

      val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef
        )

      val latestCompletedEpochLocally = EpochStore.Epoch(
        Genesis.GenesisEpochInfo, // the serving node is still at genesis
        lastBlockCommitMessages = Seq.empty,
      )

      // Handle a block transfer request.
      stateTransferManager.handleStateTransferMessage(
        NetworkMessage(
          StateTransferMessage.BlockTransferRequest.create(
            startEpoch = EpochNumber.First,
            latestCompletedEpoch = Genesis.GenesisEpochNumber,
            from = otherSequencerId,
          )
        ),
        membership,
        latestCompletedEpoch = latestCompletedEpochLocally,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

      // Should have never referenced self, e.g., to send new epoch state.
      context.selfMessages shouldBe empty

      // Should have sent a block transfer response with empty pre-prepares.
      verify(p2pNetworkOutRef, times(1))
        .asyncSend(
          P2PNetworkOut.send(
            P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
              StateTransferMessage.BlockTransferResponse
                .create(
                  latestCompletedEpochLocally.info.number,
                  prePrepares = Seq.empty,
                  from = mySequencerId,
                )
                .fakeSign
            ),
            to = otherSequencerId,
          )
        )
    }

    "send a non-empty block transfer response on a request for onboarding" in {
      implicit val context: ContextType = new ProgrammableUnitTestContext()

      val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
      val epochStore = new InMemoryUnitTestEpochStore[ProgrammableUnitTestEnv]
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef,
          epochStore = epochStore,
        )

      // Store a block that will be sent by the serving node.
      val prePrepare = aPrePrepare()
      context.pipeToSelf(
        epochStore.addOrderedBlock(prePrepare, commitMessages = Seq.empty)
      )(
        _.map(_ => None).getOrElse(fail("Storing the pre-prepare failed"))
      )
      context.runPipedMessages() // store ordered block data

      val latestCompletedEpochLocally = EpochStore.Epoch(
        EpochInfo.mk(EpochNumber.First, startBlockNumber = BlockNumber.First, length = 1),
        lastBlockCommitMessages = Seq.empty,
      )

      // Handle a block transfer request.
      stateTransferManager.handleStateTransferMessage(
        NetworkMessage(
          StateTransferMessage.BlockTransferRequest.create(
            startEpoch = EpochNumber.First,
            latestCompletedEpoch = Genesis.GenesisEpochNumber,
            from = otherSequencerId,
          )
        ),
        membership,
        latestCompletedEpoch = latestCompletedEpochLocally,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

      context.runPipedMessages() // retrieve blocks

      // Should have never referenced self, e.g., to send new epoch state.
      context.selfMessages shouldBe empty

      // Should have sent a block transfer response with a single epoch containing a single block.
      verify(p2pNetworkOutRef, times(1))
        .asyncSend(
          P2PNetworkOut.send(
            P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
              StateTransferMessage.BlockTransferResponse
                .create(
                  latestCompletedEpochLocally.info.number,
                  Seq(prePrepare),
                  from = mySequencerId,
                )
                .fakeSign
            ),
            to = otherSequencerId,
          )
        )
    }
  }

  "store, then send block to Output and complete block transfer on response for onboarding" in {
    implicit val context: ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
      new ProgrammableUnitTestContext()

    val outputRef = mock[ModuleRef[Output.Message[ProgrammableUnitTestEnv]]]
    val stateTransferManager =
      createStateTransferManager[ProgrammableUnitTestEnv](
        outputModuleRef = outputRef,
        p2pNetworkOutModuleRef = fakeIgnoringModule,
      )

    // Initiate state transfer so that it's in progress.
    val latestCompletedEpochLocally = Genesis.GenesisEpoch
    stateTransferManager.startStateTransfer(
      membership,
      latestCompletedEpochLocally,
      startEpoch = EpochNumber.First,
    )(abort = fail(_))

    val blockMetadata = BlockMetadata.mk(EpochNumber.First, BlockNumber.First)
    val prePrepare = aPrePrepare(blockMetadata)
    val latestCompletedEpochRemotely =
      EpochStore.Epoch(
        EpochInfo.mk(EpochNumber.First, startBlockNumber = BlockNumber.First, length = 1),
        lastBlockCommitMessages = Seq.empty, // not used
      )

    // Handle a block transfer response with a single epoch containing a single block.
    val blockTransferResponse = StateTransferMessage.BlockTransferResponse.create(
      latestCompletedEpochRemotely.info.number,
      Seq(prePrepare),
      from = otherSequencerId,
    )
    stateTransferManager.handleStateTransferMessage(
      NetworkMessage(blockTransferResponse),
      membership,
      latestCompletedEpochLocally,
    )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

    // Store the block.
    val blockStoredMessage = context.runPipedMessages()
    blockStoredMessage should contain only StateTransferMessage.BlocksStored(
      Seq(prePrepare.message),
      latestCompletedEpochRemotely.info.number,
    )

    val result = stateTransferManager.handleStateTransferMessage(
      blockStoredMessage.headOption
        .getOrElse(fail("There should be just a single block stored message"))
        .asInstanceOf[StateTransferMessage.BlocksStored[ProgrammableUnitTestEnv]],
      membership,
      latestCompletedEpochLocally,
    )(fail(_))

    // Should have completed the block transfer and returned a new epoch state to the Consensus module.
    val latestStateTransferredEpochInfo = latestCompletedEpochRemotely.info
      .copy(topologyActivationTime = membership.orderingTopology.activationTime)
    result shouldBe StateTransferMessageResult.BlockTransferCompleted(
      EpochState.Epoch(
        latestStateTransferredEpochInfo,
        membership,
        DefaultLeaderSelectionPolicy,
      ),
      latestCompletedEpochRemotely.copy(info = latestStateTransferredEpochInfo),
    )

    // Should have sent an ordered block to the Output module.
    verify(outputRef, times(1)).asyncSend(
      Output.BlockOrdered(
        OrderedBlockForOutput(
          OrderedBlock(
            blockMetadata,
            prePrepare.message.block.proofs,
            prePrepare.message.canonicalCommitSet,
          ),
          from = prePrepare.from,
          isLastInEpoch = true,
          mode = OrderedBlockForOutput.Mode.StateTransfer.LastBlock,
        )
      )
    )
  }

  "handle empty block transfer response" in {
    implicit val context: ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
      new ProgrammableUnitTestContext()

    val stateTransferManager =
      createStateTransferManager[ProgrammableUnitTestEnv](
        p2pNetworkOutModuleRef = fakeIgnoringModule
      )

    // Initiate state transfer so that it's in progress.
    val latestCompletedEpochLocally = Genesis.GenesisEpoch
    stateTransferManager.startStateTransfer(
      membership,
      latestCompletedEpochLocally,
      startEpoch = EpochNumber.First,
    )(abort = fail(_))

    val latestCompletedEpochRemotely =
      EpochStore.Epoch(
        Genesis.GenesisEpochInfo, // the serving node is still at genesis
        lastBlockCommitMessages = Seq.empty,
      )

    // Handle an empty block transfer response.
    val blockTransferResponse = StateTransferMessage.BlockTransferResponse.create(
      latestCompletedEpochRemotely.info.number,
      prePrepares = Seq.empty,
      from = otherSequencerId,
    )
    stateTransferManager.handleStateTransferMessage(
      NetworkMessage(blockTransferResponse),
      membership,
      latestCompletedEpochLocally,
    )(abort = fail(_)) shouldBe StateTransferMessageResult.NothingToStateTransfer
  }

  "drop messages when not in state transfer" in {
    implicit val context: ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
      new ProgrammableUnitTestContext()

    // Create the manager and don't start any state transfer.
    val stateTransferManager =
      createStateTransferManager[ProgrammableUnitTestEnv](p2pNetworkOutModuleRef =
        fakeModuleExpectingSilence // we don't expect any outbound communication
      )

    val latestCompletedEpochLocally = Genesis.GenesisEpoch
    val latestCompletedEpochRemotely =
      EpochStore.Epoch(
        EpochInfo.mk(EpochNumber.First, startBlockNumber = BlockNumber.First, length = 1),
        lastBlockCommitMessages = Seq.empty,
      )

    val aBlockTransferResponse = StateTransferMessage.BlockTransferResponse.create(
      latestCompletedEpochRemotely.info.number,
      prePrepares = Seq.empty,
      from = otherSequencerId,
    )

    forAll(
      List[StateTransferMessage](
        NetworkMessage(aBlockTransferResponse),
        StateTransferMessage.BlocksStored(
          prePrepares = Seq.empty,
          latestCompletedEpochRemotely.info.number,
        ),
      )
    ) { message =>
      stateTransferManager.handleStateTransferMessage(
        message,
        membership,
        latestCompletedEpochLocally,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

      context.runPipedMessages() should be(empty)
    }
  }

  private def createStateTransferManager[E <: BaseIgnoringUnitTestEnv[E]](
      outputModuleRef: ModuleRef[Output.Message[E]] = fakeModuleExpectingSilence,
      p2pNetworkOutModuleRef: ModuleRef[P2PNetworkOut.Message],
      epochLength: Long = 1L,
      epochStore: EpochStore[E] = new InMemoryUnitTestEpochStore[E],
  ): StateTransferManager[E] = {
    val dependencies = ConsensusModuleDependencies[E](
      availability = fakeIgnoringModule,
      outputModuleRef,
      p2pNetworkOutModuleRef,
    )

    new StateTransferManager(
      dependencies,
      EpochLength(epochLength),
      epochStore,
      mySequencerId,
      loggerFactory,
    )
  }

  private def assertBlockTransferRequestHasBeenSent(
      p2pNetworkOutRef: ModuleRef[P2PNetworkOut.Message],
      blockTransferRequest: SignedMessage[StateTransferMessage.BlockTransferRequest],
      to: SequencerId,
      numberOfTimes: Int,
  )(implicit context: ContextType): Unit = {
    // Should have scheduled a retry.
    context.lastDelayedMessage shouldBe Some(
      numberOfTimes -> StateTransferMessage.ResendBlockTransferRequest(
        blockTransferRequest,
        to = otherSequencerId,
      )
    )
    // Should have sent a block transfer request to the other peer only.
    val order = inOrder(p2pNetworkOutRef)
    order
      .verify(p2pNetworkOutRef, times(numberOfTimes))
      .asyncSend(
        P2PNetworkOut.send(
          P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(blockTransferRequest),
          to,
        )
      )
    order.verify(p2pNetworkOutRef, never).asyncSend(any[P2PNetworkOut.Message])
  }
}

object StateTransferManagerTest {
  private type ContextType = ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]]

  private val mySequencerId = fakeSequencerId("self")
  private val otherSequencerId = fakeSequencerId("other")
  private val membership = Membership(mySequencerId, Set(otherSequencerId))

  private def aPrePrepare(
      blockMetadata: BlockMetadata = BlockMetadata.mk(EpochNumber.First, BlockNumber.First)
  ) =
    PrePrepare
      .create(
        blockMetadata = blockMetadata,
        viewNumber = ViewNumber.First,
        localTimestamp = CantonTimestamp.Epoch,
        block = OrderingBlock(Seq.empty),
        canonicalCommitSet = CanonicalCommitSet.empty,
        from = otherSequencerId,
      )
      .fakeSign
}
