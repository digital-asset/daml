// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultLeaderSelectionPolicy
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  Genesis,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferManager
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferManager.{
  NewEpochState,
  NoEpochStateUpdates,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.{
  OutputBlockMetadataStore,
  OutputBlocksReader,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.NetworkMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss.{
  InMemoryUnitTestEpochStore,
  InMemoryUnitTestOutputBlockMetadataStore,
}
import com.digitalasset.canton.topology.SequencerId
import org.scalatest.wordspec.AnyWordSpec

class StateTransferManagerTest extends AnyWordSpec with BftSequencerBaseTest {
  import StateTransferManagerTest.*

  "StateTransferManager" should {
    "start, try to restart, and clear state transfer" in {
      implicit val context: ContextType = new ProgrammableUnitTestContext

      val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef
        )

      stateTransferManager.isInStateTransfer shouldBe false

      val startEpoch = EpochNumber(7L)
      stateTransferManager.startStateTransfer(
        membership,
        latestCompletedEpoch = genesisEpoch,
        startEpoch,
      )(abort = fail(_))

      stateTransferManager.isInStateTransfer shouldBe true

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
        latestCompletedEpoch = genesisEpoch,
        startEpoch,
      )(abort = fail(_))
      context.extractSelfMessages() shouldBe empty

      stateTransferManager.clearStateTransferState()
      stateTransferManager.isInStateTransfer shouldBe false
    }

    "schedule a retry and send block transfer request" in {
      implicit val context: ContextType = new ProgrammableUnitTestContext()

      val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef
        )

      // Initiate state transfer so that it's in progress.
      val latestCompletedEpoch = genesisEpoch
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
        StateTransferMessage.SendBlockTransferRequest(blockTransferRequest, to = otherSequencerId),
        membership,
        latestCompletedEpoch,
      )(completeInit = () => (), abort = fail(_)) shouldBe NoEpochStateUpdates

      assertBlockTransferRequestHasBeenSent(
        p2pNetworkOutRef,
        blockTransferRequest,
        to = otherSequencerId,
        numberOfTimes = 2, // +1 from start
      )
    }

    "handle block transfer request for onboarding" in {
      implicit val context: ContextType = new ProgrammableUnitTestContext()

      val p2pNetworkOutRef = mock[ModuleRef[P2PNetworkOut.Message]]
      val epochStore = new InMemoryUnitTestEpochStore[ProgrammableUnitTestEnv]
      val outputBlocksMetadataStore =
        new InMemoryUnitTestOutputBlockMetadataStore[ProgrammableUnitTestEnv]
      val stateTransferManager =
        createStateTransferManager[ProgrammableUnitTestEnv](
          p2pNetworkOutModuleRef = p2pNetworkOutRef,
          epochStore = epochStore,
          outputBlocksMetadataStore = outputBlocksMetadataStore,
        )

      // Store a pre-prepare that will be sent by the serving node.
      val prePrepare = aPrePrepare()
      val blockTransferData = StateTransferMessage.BlockTransferData.create(
        prePrepare,
        pendingTopologyChanges = true,
      )
      context.pipeToSelf(epochStore.addOrderedBlock(prePrepare, Seq.empty))(
        _.map(_ => None).getOrElse(fail("Storing the pre-prepare failed"))
      )
      context.pipeToSelf(
        outputBlocksMetadataStore.insertIfMissing(
          OutputBlockMetadata(
            epochNumber = EpochNumber.First,
            blockNumber = BlockNumber.First,
            blockBftTime = CantonTimestamp.Epoch,
            epochCouldAlterSequencingTopology = true,
            pendingTopologyChangesInNextEpoch = true,
          )
        )
      )(
        _.map(_ => None).getOrElse(fail("Storing the output block metadata failed"))
      )
      context.runPipedMessages() // store ordered and output block data

      // Handle a block transfer request from genesis.
      val latestCompletedEpochLocally = EpochStore.Epoch(
        EpochInfo.mk(EpochNumber.First, startBlockNumber = BlockNumber.First, length = 1),
        lastBlockCommitMessages = Seq.empty,
      )
      val latestCompletedEpochRemotely = Genesis.GenesisEpochNumber

      stateTransferManager.handleStateTransferMessage(
        NetworkMessage(
          StateTransferMessage.BlockTransferRequest
            .create(
              startEpoch = EpochNumber.First,
              latestCompletedEpoch = latestCompletedEpochRemotely,
              from = otherSequencerId,
            )
        ),
        membership,
        latestCompletedEpoch = latestCompletedEpochLocally,
      )(completeInit = () => (), abort = fail(_)) shouldBe NoEpochStateUpdates

      context.runPipedMessages() // retrieve pre-prepares and output metadata

      // Should have never referenced self, e.g., to send new epoch state.
      context.selfMessages shouldBe empty

      // Should have sent a block transfer response with a single epoch containing a single block.
      verify(p2pNetworkOutRef, times(1))
        .asyncSend(
          P2PNetworkOut.send(
            P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
              StateTransferMessage.BlockTransferResponse
                .create(
                  latestCompletedEpoch = EpochNumber.First,
                  Seq(blockTransferData),
                  lastBlockCommits = Seq.empty,
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
    val latestCompletedEpochLocally = genesisEpoch
    stateTransferManager.startStateTransfer(
      membership,
      latestCompletedEpochLocally,
      startEpoch = EpochNumber.First,
    )(abort = fail(_))

    val latestCompletedEpochRemotely =
      EpochStore.Epoch(
        EpochInfo.mk(EpochNumber.First, startBlockNumber = BlockNumber.First, length = 1),
        lastBlockCommitMessages = Seq.empty,
      )
    val blockMetadata = BlockMetadata.mk(EpochNumber.First, BlockNumber.First)
    val prePrepare = aPrePrepare(blockMetadata)
    val blockTransferData = StateTransferMessage.BlockTransferData.create(
      prePrepare,
      pendingTopologyChanges = true,
    )

    // Handle a block transfer response with a single epoch containing a single block.
    val blockTransferResponse = StateTransferMessage.BlockTransferResponse.create(
      latestCompletedEpochRemotely.info.number,
      Seq(blockTransferData),
      lastBlockCommits = Seq.empty,
      from = otherSequencerId,
    )
    stateTransferManager.handleStateTransferMessage(
      NetworkMessage(blockTransferResponse),
      membership,
      latestCompletedEpochLocally,
    )(completeInit = () => (), abort = fail(_)) shouldBe NoEpochStateUpdates

    val messages = context.runPipedMessages() // store block
    messages should contain only StateTransferMessage.BlockStored(
      blockTransferData,
      blockTransferResponse,
    )

    val newEpochState = stateTransferManager.handleStateTransferMessage(
      messages.headOption
        .getOrElse(fail("There should be a single message"))
        .asInstanceOf[StateTransferMessage],
      membership,
      latestCompletedEpochLocally,
    )(() => (), fail(_))

    // Should have completed the block transfer and returned a new epoch state to the Consensus module.
    newEpochState shouldBe Some(
      NewEpochState(
        EpochState.Epoch(
          latestCompletedEpochRemotely.info,
          membership,
          DefaultLeaderSelectionPolicy,
        ),
        latestCompletedEpochRemotely,
      )
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
          mode = OrderedBlockForOutput.Mode.StateTransfer.LastBlock(
            pendingTopologyChangesInNextEpoch = true
          ),
        )
      )
    )
  }

  private def createStateTransferManager[E <: BaseIgnoringUnitTestEnv[E]](
      outputModuleRef: ModuleRef[Output.Message[E]] = fakeModuleExpectingSilence,
      p2pNetworkOutModuleRef: ModuleRef[P2PNetworkOut.Message] = fakeModuleExpectingSilence,
      epochLength: Long = 1L,
      epochStore: EpochStore[E] = new InMemoryUnitTestEpochStore[E],
      outputBlocksMetadataStore: OutputBlockMetadataStore[E] & OutputBlocksReader[E] =
        new InMemoryUnitTestOutputBlockMetadataStore[E],
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
      outputBlocksMetadataStore,
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
      numberOfTimes -> StateTransferMessage.SendBlockTransferRequest(
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

  private val genesisEpoch: EpochStore.Epoch =
    EpochStore.Epoch(Genesis.GenesisEpochInfo, lastBlockCommitMessages = Seq.empty)
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
        canonicalCommitSet = CanonicalCommitSet(Set.empty),
        from = mySequencerId,
      )
      .fakeSign
}
