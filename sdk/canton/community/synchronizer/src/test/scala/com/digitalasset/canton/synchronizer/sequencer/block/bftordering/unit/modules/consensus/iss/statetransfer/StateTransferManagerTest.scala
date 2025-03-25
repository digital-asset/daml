// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  Genesis,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.{
  StateTransferManager,
  StateTransferMessageResult,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
  OrderingTopologyInfo,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.VerifiedStateTransferMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.InMemoryUnitTestEpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer.StateTransferTestHelpers.{
  aBlockMetadata,
  aCommitCert,
  myId,
  otherId,
}
import org.scalatest.wordspec.AnyWordSpec

class StateTransferManagerTest extends AnyWordSpec with BftSequencerBaseTest {
  import StateTransferManagerTest.*

  // TODO(#24524) test `stateTransferNewEpoch`
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
      stateTransferManager.startCatchUp(
        aMembership,
        ProgrammableUnitTestEnv.noSignatureCryptoProvider,
        latestCompletedEpoch = Genesis.GenesisEpoch,
        startEpoch,
      )(abort = fail(_))
      context.runPipedMessages()

      stateTransferManager.inStateTransfer shouldBe true

      val blockTransferRequest = StateTransferMessage.BlockTransferRequest
        .create(startEpoch, from = myId)
        .fakeSign

      assertBlockTransferRequestHasBeenSent(
        p2pNetworkOutRef,
        blockTransferRequest,
        to = otherId,
        numberOfTimes = 1,
      )

      // Try to start state transfer (with no effect) while another one is in progress.
      stateTransferManager.startCatchUp(
        aMembership,
        failingCryptoProvider,
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
      stateTransferManager.startCatchUp(
        aMembership,
        ProgrammableUnitTestEnv.noSignatureCryptoProvider,
        latestCompletedEpoch,
        startEpoch,
      )(abort = fail(_))
      context.runPipedMessages() shouldBe List()

      val blockTransferRequest = StateTransferMessage.BlockTransferRequest
        .create(startEpoch, from = myId)
        .fakeSign
      stateTransferManager.handleStateTransferMessage(
        StateTransferMessage.RetryBlockTransferRequest(blockTransferRequest),
        aTopologyInfo,
        latestCompletedEpoch,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue
      context.runPipedMessages()

      assertBlockTransferRequestHasBeenSent(
        p2pNetworkOutRef,
        blockTransferRequest,
        to = otherId,
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
        lastBlockCommits = Seq.empty,
      )

      // Handle a block transfer request.
      stateTransferManager.handleStateTransferMessage(
        VerifiedStateTransferMessage(
          StateTransferMessage.BlockTransferRequest.create(
            epoch = EpochNumber.First,
            from = otherId,
          )
        ),
        aTopologyInfo,
        latestCompletedEpoch = latestCompletedEpochLocally,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

      // Should have never referenced self, e.g., to send new epoch state.
      context.selfMessages shouldBe empty
      context.runPipedMessages()

      // Should have sent a block transfer response with an empty commit certificate.
      verify(p2pNetworkOutRef, times(1))
        .asyncSend(
          P2PNetworkOut.send(
            P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
              StateTransferMessage.BlockTransferResponse
                .create(
                  commitCertificate = None,
                  latestCompletedEpochLocally.info.number,
                  from = myId,
                )
                .fakeSign
            ),
            to = otherId,
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
      val commitCert = aCommitCert()
      context.pipeToSelf(
        epochStore.addOrderedBlock(commitCert.prePrepare, commitCert.commits)
      )(
        _.map(_ => None).getOrElse(fail("Storing the pre-prepare failed"))
      )
      context.runPipedMessages() // store ordered block data

      val latestCompletedEpochLocally = EpochStore.Epoch(
        EpochInfo.mk(EpochNumber.First, startBlockNumber = BlockNumber.First, length = 1),
        lastBlockCommits = Seq.empty,
      )

      // Handle a block transfer request.
      stateTransferManager.handleStateTransferMessage(
        VerifiedStateTransferMessage(
          StateTransferMessage.BlockTransferRequest.create(
            epoch = EpochNumber.First,
            from = otherId,
          )
        ),
        aTopologyInfo,
        latestCompletedEpoch = latestCompletedEpochLocally,
      )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

      context.runPipedMessages() // retrieve blocks
      context.runPipedMessages() // sign message

      // Should have never referenced self, e.g., to send new epoch state.
      context.selfMessages shouldBe empty

      // Should have sent a block transfer response with a single commit certificate.
      verify(p2pNetworkOutRef, times(1))
        .asyncSend(
          P2PNetworkOut.send(
            P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(
              StateTransferMessage.BlockTransferResponse
                .create(
                  Some(commitCert),
                  latestCompletedEpochLocally.info.number,
                  from = myId,
                )
                .fakeSign
            ),
            to = otherId,
          )
        )
    }
  }

  "verify, store, send block to Output, and continue on response for onboarding" in {
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
    stateTransferManager.startCatchUp(
      aMembership,
      ProgrammableUnitTestEnv.noSignatureCryptoProvider,
      latestCompletedEpochLocally,
      startEpoch = EpochNumber.First,
    )(abort = fail(_))
    context.runPipedMessages()

    val blockMetadata = aBlockMetadata
    val commitCert = aCommitCert(blockMetadata)
    val latestCompletedEpochRemotely =
      EpochStore.Epoch(
        EpochInfo.mk(EpochNumber.First, startBlockNumber = BlockNumber.First, length = 1),
        lastBlockCommits = Seq.empty, // not used
      )

    // Handle a block transfer response with a single commit certificate.
    val blockTransferResponse = StateTransferMessage.BlockTransferResponse.create(
      Some(commitCert),
      latestCompletedEpochRemotely.info.number,
      from = otherId,
    )
    val topologyInfo = OrderingTopologyInfo(
      myId,
      aMembershipBeforeOnboarding.orderingTopology,
      ProgrammableUnitTestEnv.noSignatureCryptoProvider,
      aMembershipBeforeOnboarding.leaders,
      previousTopology = aMembershipBeforeOnboarding.orderingTopology,
      previousCryptoProvider = failingCryptoProvider,
      aMembershipBeforeOnboarding.leaders,
    )
    stateTransferManager.handleStateTransferMessage(
      VerifiedStateTransferMessage(blockTransferResponse),
      topologyInfo,
      latestCompletedEpochLocally,
    )(abort = fail(_)) shouldBe StateTransferMessageResult.Continue

    // Verify the block.
    val blockVerifiedMessage = context.runPipedMessages()
    blockVerifiedMessage should contain only StateTransferMessage.BlockVerified(
      commitCert,
      latestCompletedEpochRemotely.info.number,
      from = otherId,
    )
    stateTransferManager.handleStateTransferMessage(
      blockVerifiedMessage.headOption
        .getOrElse(fail("There should be just a single block verified message"))
        .asInstanceOf[StateTransferMessage.BlockVerified[ProgrammableUnitTestEnv]],
      topologyInfo,
      latestCompletedEpochLocally,
    )(fail(_))

    // Store the block.
    val blockStoredMessage = context.runPipedMessages()
    blockStoredMessage should contain only StateTransferMessage.BlockStored(
      commitCert,
      latestCompletedEpochRemotely.info.number,
      from = otherId,
    )

    val result = stateTransferManager.handleStateTransferMessage(
      blockStoredMessage.headOption
        .getOrElse(fail("There should be just a single block stored message"))
        .asInstanceOf[StateTransferMessage.BlockStored[ProgrammableUnitTestEnv]],
      topologyInfo,
      latestCompletedEpochLocally,
    )(fail(_))

    result shouldBe StateTransferMessageResult.Continue

    // Should have sent an ordered block to the Output module.
    val prePrepare = commitCert.prePrepare.message
    verify(outputRef, times(1)).asyncSend(
      Output.BlockOrdered(
        OrderedBlockForOutput(
          OrderedBlock(
            blockMetadata,
            prePrepare.block.proofs,
            prePrepare.canonicalCommitSet,
          ),
          prePrepare.viewNumber,
          from = commitCert.prePrepare.from,
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
    stateTransferManager.startCatchUp(
      aMembership,
      ProgrammableUnitTestEnv.noSignatureCryptoProvider,
      latestCompletedEpochLocally,
      startEpoch = EpochNumber.First,
    )(abort = fail(_))

    val latestCompletedEpochRemotely =
      EpochStore.Epoch(
        Genesis.GenesisEpochInfo, // the serving node is still at genesis
        lastBlockCommits = Seq.empty,
      )

    // Handle an empty block transfer response.
    val blockTransferResponse = StateTransferMessage.BlockTransferResponse.create(
      commitCertificate = None,
      latestCompletedEpochRemotely.info.number,
      from = otherId,
    )
    context.runPipedMessages() shouldBe List()
    stateTransferManager.handleStateTransferMessage(
      VerifiedStateTransferMessage(blockTransferResponse),
      aTopologyInfo,
      latestCompletedEpochLocally,
    )(abort = fail(_)) shouldBe StateTransferMessageResult.NothingToStateTransfer(from = otherId)
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
        lastBlockCommits = Seq.empty,
      )

    val aBlockTransferResponse = StateTransferMessage.BlockTransferResponse.create(
      commitCertificate = None,
      latestCompletedEpochRemotely.info.number,
      from = otherId,
    )

    forAll(
      List[StateTransferMessage](
        VerifiedStateTransferMessage(aBlockTransferResponse),
        StateTransferMessage.BlockStored(
          aCommitCert(),
          latestCompletedEpochRemotely.info.number,
          from = otherId,
        ),
      )
    ) { message =>
      stateTransferManager.handleStateTransferMessage(
        message,
        aTopologyInfo,
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
      myId,
      dependencies,
      EpochLength(epochLength),
      epochStore,
      loggerFactory,
    )
  }

  private def assertBlockTransferRequestHasBeenSent(
      p2pNetworkOutRef: ModuleRef[P2PNetworkOut.Message],
      blockTransferRequest: SignedMessage[StateTransferMessage.BlockTransferRequest],
      to: BftNodeId,
      numberOfTimes: Int,
  )(implicit context: ContextType): Unit = {
    // Should have scheduled a retry.
    context.lastDelayedMessage shouldBe Some(
      numberOfTimes -> StateTransferMessage.RetryBlockTransferRequest(blockTransferRequest)
    )
    // Should have sent a block transfer request to the other node only.
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

  private val aMembership = Membership.forTesting(myId, Set(otherId))
  private val aMembershipBeforeOnboarding =
    Membership(
      myId,
      OrderingTopology.forTesting(Set(otherId), SequencingParameters.Default),
      Seq(otherId),
    )
  private val aTopologyInfo = OrderingTopologyInfo[ProgrammableUnitTestEnv](
    myId,
    aMembership.orderingTopology,
    ProgrammableUnitTestEnv.noSignatureCryptoProvider,
    aMembership.leaders,
    previousTopology = aMembership.orderingTopology,
    previousCryptoProvider = failingCryptoProvider,
    aMembership.leaders,
  )
}
