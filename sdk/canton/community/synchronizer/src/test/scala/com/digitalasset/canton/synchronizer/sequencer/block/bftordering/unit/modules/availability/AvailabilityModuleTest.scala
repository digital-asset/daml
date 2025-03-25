// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.availability

import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.{Hash, Signature, SignatureCheckError}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule.quorum
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModuleConfig.EmptyBlockCreationInterval
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.memory.GenericInMemoryAvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  BatchId,
  InProgressBatchMetadata,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequest,
  OrderingRequestBatch,
  OrderingRequestBatchStats,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.LocalDissemination.LocalBatchStoredSigned
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.{
  LocalDissemination,
  LocalOutputFetch,
  RemoteDissemination,
  RemoteOutputFetch,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.LocalAvailability.ProposalCreated
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.AvailabilityModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Mempool,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.UnitTestContext.DelayCount
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*
import scala.util.{Random, Try}

class AvailabilityModuleTest extends AnyWordSpec with BftSequencerBaseTest {

  private val Node0 = node(0)
  private val Node1 = node(1)
  private val Node2 = node(2)
  private val Node3 = node(3)
  private val Node1And2 = (1 to 2).map(node).toSet
  private val Node0To3 = (0 to 3).map(node).toSet
  private val Node1To3 = (1 to 3).map(node).toSet
  private val Node1To6 = (1 to 6).map(node).toSet
  private val AnotherBatchId = BatchId.createForTesting("AnotherBatchId")
  private val anEpochNumber = EpochNumber.First
  private val anOrderingRequest: Traced[OrderingRequest] = Traced(
    OrderingRequest("tag", ByteString.EMPTY)
  )
  private val ABatch = OrderingRequestBatch.create(
    Seq(anOrderingRequest),
    anEpochNumber,
  )
  private val ABatchId = BatchId.from(ABatch)
  private val AnInProgressBatchMetadata =
    InProgressBatchMetadata(ABatchId, anEpochNumber, ABatch.stats)
  private val WrongBatchId = BatchId.createForTesting("Wrong BatchId")
  private val ABlockMetadata: BlockMetadata =
    BlockMetadata.mk(
      epochNumber = 0,
      blockNumber = 0,
    )
  private val AnOrderedBlockForOutput = OrderedBlockForOutput(
    OrderedBlock(
      ABlockMetadata,
      Seq(ProofOfAvailability(ABatchId, Seq.empty, anEpochNumber)),
      CanonicalCommitSet(Set.empty),
    ),
    ViewNumber.First,
    isLastInEpoch = false, // Irrelevant for availability
    mode = OrderedBlockForOutput.Mode.FromConsensus,
    from = Node0,
  )
  private val AnotherOrderedBlockForOutput = OrderedBlockForOutput(
    OrderedBlock(
      ABlockMetadata,
      Seq(
        ProofOfAvailability(ABatchId, Seq.empty, anEpochNumber),
        ProofOfAvailability(AnotherBatchId, Seq.empty, anEpochNumber),
      ),
      CanonicalCommitSet(Set.empty),
    ),
    ViewNumber.First,
    isLastInEpoch = false, // Irrelevant for availability
    mode = OrderedBlockForOutput.Mode.FromConsensus,
    from = Node0,
  )
  private val ACompleteBlock = CompleteBlockData(
    AnOrderedBlockForOutput,
    Seq(ABatchId -> ABatch),
  )
  private val Node0Ack = AvailabilityAck(from = Node0, Signature.noSignature)
  private val Node0Acks = Set(Node0Ack)
  private val Node0And1Acks = Seq(
    AvailabilityAck(from = Node0, Signature.noSignature),
    AvailabilityAck(from = Node1, Signature.noSignature),
  )
  private val FirstFourNodesQuorumAcks = (0 until quorum(numberOfNodes = 4)).map { idx =>
    AvailabilityAck(from = node(idx), Signature.noSignature)
  }
  private val Nodes0And4To6QuorumAcks =
    (Range.inclusive(0, 0) ++ (4 until quorum(numberOfNodes = 7))).map { idx =>
      AvailabilityAck(from = node(idx), Signature.noSignature)
    }
  private val Node1And2Acks = Seq(
    AvailabilityAck(from = Node1, Signature.noSignature),
    AvailabilityAck(from = Node2, Signature.noSignature),
  )
  private val OrderingTopologyNode0 = OrderingTopology.forTesting(Set(Node0))
  private val ADisseminationProgressNode0WithNode0Vote =
    DisseminationProgress(
      OrderingTopologyNode0,
      AnInProgressBatchMetadata,
      acks = Node0Acks,
    )
  private val OrderingTopologyNodes0And1 = OrderingTopology.forTesting(Set(Node0, Node1))

  private val ADisseminationProgressNode0And1WithNode0Vote =
    ADisseminationProgressNode0WithNode0Vote.copy(
      orderingTopology = OrderingTopologyNodes0And1
    )
  private val ADisseminationProgressNode0And1WithNode0And1Votes =
    DisseminationProgress(
      OrderingTopologyNodes0And1,
      AnInProgressBatchMetadata,
      acks = Node0And1Acks.toSet,
    )
  private val OrderingTopologyNodes0To3 = OrderingTopology.forTesting(Node0To3)
  private val ADisseminationProgressNode0To3WithNode0Vote =
    ADisseminationProgressNode0WithNode0Vote.copy(
      orderingTopology = OrderingTopologyNodes0To3
    )
  private val OrderingTopologyWithNode0To6 = OrderingTopology.forTesting(Node1To6 + Node0)
  private val ADisseminationProgressNode0To6WithNode0Vote =
    ADisseminationProgressNode0WithNode0Vote.copy(
      orderingTopology = OrderingTopologyWithNode0To6
    )
  private val ADisseminationProgressNode0To6WithNode0And1Vote =
    ADisseminationProgressNode0And1WithNode0And1Votes.copy(
      orderingTopology = OrderingTopologyWithNode0To6
    )
  private val QuorumAcksForNode0To3 =
    (0 until quorum(numberOfNodes = 4)).map { idx =>
      remoteBatchAcknowledged(idx)
    }
  private val NonQuorumAcksForNode0To6 =
    (0 until quorum(numberOfNodes = 7) - 1).map { idx =>
      remoteBatchAcknowledged(idx)
    }
  private val ADisseminationProgressNode0To6WithNonQuorumVotes =
    ADisseminationProgressNode0To6WithNode0Vote.copy(
      acks = (0 until quorum(numberOfNodes = 7) - 1).map { idx =>
        AvailabilityAck(from = node(idx), Signature.noSignature)
      }.toSet
    )
  private val ABatchDisseminationProgressNode0And1WithNode0Vote =
    ABatchId -> ADisseminationProgressNode0And1WithNode0Vote
  private val ABatchDisseminationProgressNode0To3WithNode0Vote =
    ABatchId -> ADisseminationProgressNode0To3WithNode0Vote
  private val ABatchDisseminationProgressNode0To6WithNode0Vote =
    ABatchId -> ADisseminationProgressNode0To6WithNode0Vote
  private val ABatchDisseminationProgressNode0To6WithNode0And1Votes =
    ABatchId -> ADisseminationProgressNode0To6WithNode0And1Vote
  private val ABatchDisseminationProgressNode0To6WithNonQuorumVotes =
    ABatchId -> ADisseminationProgressNode0To6WithNonQuorumVotes
  private val ProofOfAvailabilityNode0AckNode0InTopology = ProofOfAvailability(
    ABatchId,
    Node0Acks.toSeq,
    anEpochNumber,
  )
  private val ProofOfAvailabilityNode0AckNode0To2InTopology = ProofOfAvailability(
    ABatchId,
    Node0Acks.toSeq,
    anEpochNumber,
  )
  private val ProofOfAvailabilityNode1And2AcksNode1And2InTopology = ProofOfAvailability(
    ABatchId,
    Node1And2Acks,
    anEpochNumber,
  )
  private val BatchReadyForOrderingNode0Vote =
    ABatchId -> InProgressBatchMetadata(
      ABatchId,
      anEpochNumber,
      ABatch.stats,
    ).complete(ProofOfAvailabilityNode0AckNode0InTopology.acks)
  private val ABatchProposalNode0VoteNode0InTopology = Consensus.LocalAvailability.ProposalCreated(
    OrderingBlock(
      Seq(ProofOfAvailabilityNode0AckNode0InTopology)
    ),
    EpochNumber.First,
  )
  private val ABatchProposalNode0VoteNodes0To2InTopology =
    Consensus.LocalAvailability.ProposalCreated(
      OrderingBlock(
        Seq(ProofOfAvailabilityNode0AckNode0To2InTopology)
      ),
      EpochNumber.First,
    )
  private val ProofOfAvailabilityNode0And1VotesNodes0And1InTopology = ProofOfAvailability(
    ABatchId,
    Node0And1Acks,
    anEpochNumber,
  )
  private val ProofOfAvailability4NodesQuorumVotesNodes0To3InTopology = ProofOfAvailability(
    ABatchId,
    FirstFourNodesQuorumAcks,
    anEpochNumber,
  )
  private val ProofOfAvailability6NodesQuorumVotesNodes0And4To6InTopology = ProofOfAvailability(
    ABatchId,
    Nodes0And4To6QuorumAcks,
    anEpochNumber,
  )
  private val BatchReadyForOrderingNode0And1Votes =
    ABatchId -> InProgressBatchMetadata(ABatchId, anEpochNumber, ABatch.stats)
      .complete(ProofOfAvailabilityNode0And1VotesNodes0And1InTopology.acks)
  private val BatchReadyForOrdering4NodesQuorumVotes =
    ABatchId -> InProgressBatchMetadata(ABatchId, anEpochNumber, ABatch.stats)
      .complete(ProofOfAvailability4NodesQuorumVotesNodes0To3InTopology.acks)
  private val AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes =
    AnotherBatchId -> InProgressBatchMetadata(AnotherBatchId, anEpochNumber, ABatch.stats)
      .complete(ProofOfAvailability6NodesQuorumVotesNodes0And4To6InTopology.acks)
  private val ABatchProposalNode0And1Votes = Consensus.LocalAvailability.ProposalCreated(
    OrderingBlock(
      Seq(ProofOfAvailabilityNode0And1VotesNodes0And1InTopology)
    ),
    EpochNumber.First,
  )
  private val ABatchProposal4NodesQuorumVotes = Consensus.LocalAvailability.ProposalCreated(
    OrderingBlock(
      Seq(ProofOfAvailability4NodesQuorumVotesNodes0To3InTopology)
    ),
    EpochNumber.First,
  )
  private val AMissingBatchStatusNode1And2AcksWithNode1ToTry =
    MissingBatchStatus(
      ABatchId,
      ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
      remainingNodesToTry = Seq(Node1),
      mode = OrderedBlockForOutput.Mode.FromConsensus,
    )
  private val AMissingBatchStatusNode1And2AcksWithNode2ToTry =
    AMissingBatchStatusNode1And2AcksWithNode1ToTry
      .copy(remainingNodesToTry =
        ProofOfAvailabilityNode1And2AcksNode1And2InTopology.acks.map(_.from).tail
      )
  private val AMissingBatchStatusNode1And2AcksWithNoAttemptsLeft =
    AMissingBatchStatusNode1And2AcksWithNode1ToTry
      .copy(remainingNodesToTry = Seq.empty)
  private val ABatchMissingBatchStatusNode1And2AcksWithNoAttemptsLeft =
    ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
  private val AMissingBatchStatusFromStateTransferWithNoAttemptsLeft =
    AMissingBatchStatusNode1And2AcksWithNoAttemptsLeft
      .copy(mode = OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock)
  private val AToBeProvidedToConsensus =
    ToBeProvidedToConsensus(
      BftBlockOrdererConfig.DefaultMaxBatchesPerProposal,
      EpochNumber.First,
    )

  private implicit val fakeTimerIgnoringUnitTestContext
      : IgnoringUnitTestContext[Availability.Message[IgnoringUnitTestEnv]] =
    IgnoringUnitTestContext()

  "the availability store" when {
    "it receives Dissemination.LocalBatchCreated (from local mempool)" should {
      "should store in the local store" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

        val availability = createAvailability[IgnoringUnitTestEnv](
          availabilityStore = availabilityStore,
          disseminationProtocolState = disseminationProtocolState,
        )
        availability.receive(
          LocalDissemination.LocalBatchCreated(Seq(anOrderingRequest))
        )

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        verify(availabilityStore, times(1)).addBatch(ABatchId, ABatch)
      }
    }
  }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are no consensus requests and " +
    "there are no other nodes (so, F == 0)" should {
      "clear dissemination progress and " +
        "mark the batch ready for ordering" in {
          implicit val ctx
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext()
          val disseminationProtocolState = new DisseminationProtocolState()

          val cryptoProvider = newMockCrypto

          val me = Node0
          val availability = createAvailability[ProgrammableUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            myId = me,
            cryptoProvider = cryptoProvider,
          )
          availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))
          ctx.runPipedMessagesAndReceiveOnModule(availability) // Perform signing

          verify(cryptoProvider).signHash(
            AvailabilityAck.hashFor(ABatchId, anEpochNumber, me)
          )

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.batchesReadyForOrdering should
            contain only BatchReadyForOrderingNode0Vote
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are no consensus requests and " +
    "F > 0" should {
      "just update dissemination progress" in {
        implicit val ctx
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext()
        val disseminationProtocolState = new DisseminationProtocolState()

        val me = Node0
        val cryptoProvider = newMockCrypto

        val availability = createAvailability[ProgrammableUnitTestEnv](
          otherNodes = Node1To3,
          myId = me,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))
        ctx.runPipedMessagesAndReceiveOnModule(availability) // Perform signing

        verify(cryptoProvider).signHash(
          AvailabilityAck.hashFor(ABatchId, anEpochNumber, me)
        )

        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To3WithNode0Vote
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are consensus requests, " +
    "there are no other nodes (so, F == 0) " should {
      "just send proposal to local consensus" in {
        implicit val ctx
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext()
        val disseminationProtocolState = new DisseminationProtocolState()
        disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
        val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
        val cryptoProvider = newMockCrypto
        val me = Node0
        val availability = createAvailability[ProgrammableUnitTestEnv](
          consensus = fakeCellModule(consensusCell),
          myId = me,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))
        ctx.runPipedMessagesAndReceiveOnModule(availability) // Perform signing

        verify(cryptoProvider).signHash(
          AvailabilityAck.hashFor(ABatchId, anEpochNumber, me)
        )

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should not be empty

        consensusCell.get() should contain(ABatchProposalNode0VoteNode0InTopology)
        availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
      }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are consensus requests, " +
    "there are other nodes and " +
    "F == 0" should {
      "send proposal to local consensus and " +
        "broadcast Dissemination.RemoteBatch" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
          val me = Node0
          val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)

          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAvailability[ProgrammableUnitTestEnv](
            otherNodes = Node1And2,
            myId = me,
            cryptoProvider = cryptoProvider,
            consensus = fakeCellModule(consensusCell),
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))

          verify(cryptoProvider).signHash(
            AvailabilityAck.hashFor(ABatchId, anEpochNumber, me)
          )

          availability.receive(
            LocalDissemination.LocalBatchesStoredSigned(
              Seq(LocalBatchStoredSigned(ABatchId, ABatch, Right(Signature.noSignature)))
            )
          )

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)

          disseminationProtocolState.batchesReadyForOrdering should not be empty
          consensusCell.get() should contain(ABatchProposalNode0VoteNodes0To2InTopology)
          availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
          disseminationProtocolState.batchesReadyForOrdering should be(empty)

          p2pNetworkOutCell.get() shouldBe None
          val remoteBatch = RemoteDissemination.RemoteBatch
            .create(ABatchId, ABatch, Node0)
          verify(cryptoProvider).signMessage(
            remoteBatch,
            AuthenticatedMessageType.BftSignedAvailabilityMessage,
          )

          context.runPipedMessagesAndReceiveOnModule(availability)

          p2pNetworkOutCell.get() should contain(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                remoteBatch.fakeSign
              ),
              Set(Node1, Node2),
            )
          )
        }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are consensus requests and " +
    "F > 0 (so, there must also be other nodes)" should {
      "update dissemination progress and " +
        "broadcast Dissemination.RemoteBatch" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
          val me = Node0
          val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)
          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAvailability[ProgrammableUnitTestEnv](
            otherNodes = Node1To3,
            myId = me,
            cryptoProvider = cryptoProvider,
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))

          verify(cryptoProvider).signHash(
            AvailabilityAck.hashFor(ABatchId, anEpochNumber, me)
          )

          availability.receive(
            LocalDissemination.LocalBatchesStoredSigned(
              Seq(LocalBatchStoredSigned(ABatchId, ABatch, Right(Signature.noSignature)))
            )
          )

          disseminationProtocolState.disseminationProgress should
            contain only ABatchDisseminationProgressNode0To3WithNode0Vote
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus
          p2pNetworkOutCell.get() shouldBe None
          val remoteBatch = RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, Node0)
          verify(cryptoProvider).signMessage(
            remoteBatch,
            AuthenticatedMessageType.BftSignedAvailabilityMessage,
          )

          context.runPipedMessagesAndReceiveOnModule(availability)

          p2pNetworkOutCell.get() should contain(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                remoteBatch.fakeSign
              ),
              Node1To3,
            )
          )
        }
    }

  "it receives Dissemination.RemoteBatch (from node)" should {
    "store in the local store" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
      )
      availability.receive(
        RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, from = Node1)
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verify(availabilityStore, times(1)).addBatch(ABatchId, ABatch)
    }

    "not store if it is the wrong batchId" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
      )
      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch.create(WrongBatchId, ABatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include("BatchId doesn't match digest")
        },
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verifyZeroInteractions(availabilityStore)
    }

    "not store if too many requests in a batch" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
        maxRequestsInBatch = 0,
      )
      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include(
            "Batch BatchId(SHA-256:f9fbd79100fb...) from 'node1' contains more requests (1) than allowed (0), skipping"
          )
        },
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verifyZeroInteractions(availabilityStore)
    }
  }

  "it receives Dissemination.RemoteBatchStored (from local store)" should {
    "just acknowledge the originating node" in {
      val disseminationProtocolState = new DisseminationProtocolState()
      val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]

      val myId = Node0
      val availability = createAvailability[IgnoringUnitTestEnv](
        myId = myId,
        disseminationProtocolState = disseminationProtocolState,
        cryptoProvider = cryptoProvider,
      )
      availability.receive(
        LocalDissemination.RemoteBatchStored(ABatchId, anEpochNumber, from = Node1)
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verify(cryptoProvider).signHash(
        AvailabilityAck.hashFor(ABatchId, anEpochNumber, myId)
      )
    }
  }

  "it receives Dissemination.RemoteBatchStoredSigned" should {
    "just acknowledge the originating node" in {
      val disseminationProtocolState = new DisseminationProtocolState()
      val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

      implicit val context
          : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext
      val availability = createAvailability[ProgrammableUnitTestEnv](
        p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
        disseminationProtocolState = disseminationProtocolState,
        cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
      )
      val signature = Signature.noSignature
      availability.receive(
        LocalDissemination.RemoteBatchStoredSigned(ABatchId, from = Node1, signature)
      )

      p2pNetworkOutCell.get() shouldBe None

      context.runPipedMessagesAndReceiveOnModule(availability)

      p2pNetworkOutCell.get() should contain(
        P2PNetworkOut.Multicast(
          P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
            RemoteDissemination.RemoteBatchAcknowledged
              .create(ABatchId, Node0, signature)
              .fakeSign
          ),
          Set(Node1),
        )
      )
    }
  }

  "it receives one Dissemination.RemoteBatchAcknowledged (from node) but " +
    "the batch is not being disseminated [anymore]" should {
      "do nothing" in {
        val disseminationProtocolState = new DisseminationProtocolState()

        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        val msg = remoteBatchAcknowledged(idx = 1)
        availability.receive(msg)

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        verifyZeroInteractions(cryptoProvider)
      }
    }

  "F == 0 (i.e., proof of availability is complete), " +
    "it receives one Dissemination.RemoteBatchAcknowledged from node, " +
    "the batch is being disseminated, " +
    "there are no consensus requests" should {
      "ignore the ACK" in {
        val disseminationProtocolState = new DisseminationProtocolState()

        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0And1WithNode0Vote
        )
        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          otherNodes = Set(Node1),
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        val msg = remoteBatchAcknowledged(idx = 1)
        availability.receive(msg)

        verify(cryptoProvider).verifySignature(
          AvailabilityAck.hashFor(msg.batchId, anEpochNumber, msg.from),
          msg.from,
          msg.signature,
        )

        availability.receive(
          LocalDissemination.RemoteBatchAcknowledgeVerified(msg.batchId, msg.from, msg.signature)
        )

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should
          contain only BatchReadyForOrderingNode0And1Votes
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      }
    }

  "F > 0, " +
    "it receives `>= quorum-1` Dissemination.RemoteBatchAcknowledged from node " +
    "(i.e., proof of availability is complete), " +
    "the batch is being disseminated and " +
    "there are no consensus requests" should {
      "reset dissemination progress and " +
        "mark the batch ready for ordering" in {
          val disseminationProtocolState = new DisseminationProtocolState()

          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0To3WithNode0Vote
          )
          val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
          val availability = createAvailability[IgnoringUnitTestEnv](
            otherNodes = Node1To3,
            cryptoProvider = cryptoProvider,
            disseminationProtocolState = disseminationProtocolState,
          )
          QuorumAcksForNode0To3.tail.foreach { quorumAck =>
            availability.receive(quorumAck)
            verify(cryptoProvider).verifySignature(
              AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from),
              quorumAck.from,
              quorumAck.signature,
            )
          }

          QuorumAcksForNode0To3.tail.foreach { quorumAck =>
            availability.receive(
              LocalDissemination.RemoteBatchAcknowledgeVerified(
                quorumAck.batchId,
                quorumAck.from,
                quorumAck.signature,
              )
            )
          }

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.batchesReadyForOrdering should
            contain only BatchReadyForOrdering4NodesQuorumVotes
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        }
    }

  "F > 0, " +
    "it receives `< quorum-1` Dissemination.RemoteBatchAcknowledged from nodes " +
    "(i.e., proof of availability is incomplete), " +
    "the batch is being disseminated, " +
    "there are no consensus requests" should {
      "just update dissemination progress" in {
        val disseminationProtocolState = new DisseminationProtocolState()

        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0To6WithNode0Vote
        )
        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          otherNodes = Node1To6,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        NonQuorumAcksForNode0To6.tail.foreach { quorumAck =>
          availability.receive(quorumAck)
          verify(cryptoProvider).verifySignature(
            AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from),
            quorumAck.from,
            quorumAck.signature,
          )
        }

        NonQuorumAcksForNode0To6.tail.foreach { quorumAck =>
          availability.receive(
            LocalDissemination.RemoteBatchAcknowledgeVerified(
              quorumAck.batchId,
              quorumAck.from,
              quorumAck.signature,
            )
          )
        }

        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To6WithNonQuorumVotes
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      }
    }

  "F == 0 (i.e., proof of availability is complete), " +
    "it receives Dissemination.RemoteBatchAcknowledged from node, " +
    "the batch is being disseminated and " +
    "there are consensus requests" should {
      "reset dissemination progress and " +
        "send proposal to local consensus" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0And1WithNode0Vote
          )
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
          val availability = createAvailability[IgnoringUnitTestEnv](
            otherNodes = Set(Node1),
            cryptoProvider = cryptoProvider,
            consensus = fakeCellModule(consensusCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          val msg = remoteBatchAcknowledged(idx = 1)
          availability.receive(msg)
          verify(cryptoProvider).verifySignature(
            AvailabilityAck.hashFor(msg.batchId, anEpochNumber, msg.from),
            msg.from,
            msg.signature,
          )

          availability.receive(
            LocalDissemination.RemoteBatchAcknowledgeVerified(msg.batchId, msg.from, msg.signature)
          )

          consensusCell.get() should contain(ABatchProposalNode0And1Votes)
          disseminationProtocolState.batchesReadyForOrdering should not be empty
          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)

          availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
        }
    }

  "F > 0, " +
    "it receives `>= quorum-1` Dissemination.RemoteBatchAcknowledged from nodes " +
    "(i.e., proof of availability is complete), " +
    "the batch is being disseminated, " +
    "there are consensus requests" should {
      "reset dissemination progress and " +
        "send proposal to local consensus" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0To3WithNode0Vote
          )
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
          val availability = createAvailability[IgnoringUnitTestEnv](
            otherNodes = Node1To3,
            cryptoProvider = cryptoProvider,
            consensus = fakeCellModule(consensusCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          QuorumAcksForNode0To3.tail.foreach { quorumAck =>
            availability.receive(quorumAck)
            verify(cryptoProvider).verifySignature(
              AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from),
              quorumAck.from,
              quorumAck.signature,
            )
            availability.receive(
              LocalDissemination.RemoteBatchAcknowledgeVerified(
                quorumAck.batchId,
                quorumAck.from,
                quorumAck.signature,
              )
            )
          }

          consensusCell.get() should contain(ABatchProposal4NodesQuorumVotes)
          disseminationProtocolState.batchesReadyForOrdering should not be empty
          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)

          availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
        }
    }

  "F > 0, " +
    "it receives `< quorum-1` Dissemination.RemoteBatchAcknowledged from nodes " +
    "(i.e., proof of availability is incomplete), " +
    "the batch is being disseminated, " +
    "there are consensus requests" should {
      "just update dissemination progress" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0To6WithNode0Vote
        )
        disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          otherNodes = Node1To6,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        NonQuorumAcksForNode0To6.tail.foreach { quorumAck =>
          availability.receive(quorumAck)

          verify(cryptoProvider).verifySignature(
            AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from),
            quorumAck.from,
            quorumAck.signature,
          )

          availability.receive(
            LocalDissemination.RemoteBatchAcknowledgeVerified(
              quorumAck.batchId,
              quorumAck.from,
              quorumAck.signature,
            )
          )
        }

        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To6WithNonQuorumVotes
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus
      }
    }

  "it receives OutputFetch.FetchBatchDataFromNodes (from local store) and " +
    "it is already fetching it" should {
      "do nothing" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()

        outputFetchProtocolState.localOutputMissingBatches.addOne(
          ABatchMissingBatchStatusNode1And2AcksWithNoAttemptsLeft
        )
        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState
        )
        availability.receive(
          LocalOutputFetch.FetchBatchDataFromNodes(
            ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
            OrderedBlockForOutput.Mode.FromConsensus,
          )
        )

        outputFetchProtocolState.localOutputMissingBatches should contain only ABatchMissingBatchStatusNode1And2AcksWithNoAttemptsLeft
        outputFetchProtocolState.incomingBatchRequests should be(empty)
      }
    }

  "it receives OutputFetch.FetchBatchDataFromNodes (from local store) and " +
    "it is not already fetching it" should {
      "update the fetch progress, " +
        "set a fetch timeout and " +
        "send OutputFetch.FetchRemoteBatchData to the currently attempted node" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAvailability[ProgrammableUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState,
            cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
          )
          availability.receive(
            LocalOutputFetch.FetchBatchDataFromNodes(
              ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
              OrderedBlockForOutput.Mode.FromConsensus,
            )
          )

          outputFetchProtocolState.localOutputMissingBatches should
            contain only ABatchId -> AMissingBatchStatusNode1And2AcksWithNode2ToTry
          outputFetchProtocolState.incomingBatchRequests should be(empty)
          context.delayedMessages should contain(
            LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId)
          )

          p2pNetworkOutCell.get() shouldBe None
          context.runPipedMessagesAndReceiveOnModule(availability)

          p2pNetworkOutCell.get() should contain(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0).fakeSign
              ),
              Set(Node1),
            )
          )
        }
    }

  "it receives OutputFetch.FetchRemoteBatchData (from node) and " +
    "there is an incoming request for the batch already" should {
      "just record the new requesting node" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()

        outputFetchProtocolState.incomingBatchRequests.addOne(ABatchId -> Set(Node1))
        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState
        )
        availability.receive(RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node2))

        outputFetchProtocolState.localOutputMissingBatches should be(empty)
        outputFetchProtocolState.incomingBatchRequests should contain only ABatchId -> Set(
          Node1,
          Node2,
        )
      }
    }

  "it receives OutputFetch.FetchRemoteBatchData (from node) and " +
    "there is no incoming request for the batch" should {
      "record the first requesting node and " +
        "fetch batch from local store" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
          val availability = createAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState,
            availabilityStore = availabilityStore,
          )
          availability.receive(RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node1))

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should contain only ABatchId -> Set(
            Node1
          )
          verify(availabilityStore).fetchBatches(Seq(ABatchId))
        }
    }

  "it receives OutputFetch.AttemptedBatchDataLoad (from local store) and " +
    "the batch was not found" should {
      "do nothing" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()

        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState
        )
        availability.receive(LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, None))

        outputFetchProtocolState.localOutputMissingBatches should be(empty)
        outputFetchProtocolState.incomingBatchRequests should be(empty)
      }
    }

  "it receives OutputFetch.AttemptedBatchDataLoad and " +
    "the batch was found and " +
    "there is no incoming request for the batch" should {
      "do nothing" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()

        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState
        )
        availability.receive(LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, Some(ABatch)))

        outputFetchProtocolState.localOutputMissingBatches should be(empty)
        outputFetchProtocolState.incomingBatchRequests should be(empty)
      }
    }

  "it receives OutputFetch.AttemptedBatchDataLoad and " +
    "the batch is found and " +
    "there is an incoming request for the batch" should {
      "send OutputFetch.RemoteBatchDataFetched to all requesting node and " +
        "remove the batch from incoming requests" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          outputFetchProtocolState.incomingBatchRequests.addOne(ABatchId -> Set(Node1))
          val availability = createAvailability[ProgrammableUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState,
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
          )
          availability.receive(
            LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, Some(ABatch))
          )

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)

          p2pNetworkOutCell.get() shouldBe None

          context.runPipedMessagesAndReceiveOnModule(availability)
          p2pNetworkOutCell.get() should contain(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                RemoteOutputFetch.RemoteBatchDataFetched
                  .create(Node0, ABatchId, ABatch)
                  .fakeSign
              ),
              Set(Node1),
            )
          )
        }
    }

  "it receives OutputFetch.AttemptedBatchDataLoad and " +
    "the batch is NOT found and " +
    "there is an incoming request for the batch" should {
      "just remove the batch from incoming requests" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()
        val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

        implicit val context
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext
        outputFetchProtocolState.incomingBatchRequests.addOne(ABatchId -> Set(Node1))
        val availability = createAvailability[ProgrammableUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState,
          p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
          cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
        )
        availability.receive(
          LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, None)
        )

        outputFetchProtocolState.localOutputMissingBatches should be(empty)
        outputFetchProtocolState.incomingBatchRequests should be(empty)

        p2pNetworkOutCell.get() shouldBe None

        context.runPipedMessagesAndReceiveOnModule(availability)
        p2pNetworkOutCell.get() shouldBe None
      }
    }

  "it receives OutputFetch.RemoteBatchDataFetched and " +
    "the batch is not missing" should {
      "do nothing" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()

        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState
        )
        availability.receive(
          RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, ABatchId, ABatch)
        )

        outputFetchProtocolState.localOutputMissingBatches should be(empty)
        outputFetchProtocolState.incomingBatchRequests should be(empty)
      }
    }

  "it receives OutputFetch.RemoteBatchDataFetched and " +
    "the batch is missing" should {
      "just store the batch in the local store" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()
        val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

        outputFetchProtocolState.localOutputMissingBatches.addOne(
          ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
        )
        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState,
          availabilityStore = availabilityStore,
        )
        availability.receive(
          RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, ABatchId, ABatch)
        )

        outputFetchProtocolState.localOutputMissingBatches should contain only ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
        outputFetchProtocolState.incomingBatchRequests should be(empty)
        verify(availabilityStore).addBatch(ABatchId, ABatch)
      }
    }

  "it receives OutputFetch.FetchedBatchStored and " +
    "the batch is not missing" should {
      "do nothing" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()

        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState
        )
        availability.receive(LocalOutputFetch.FetchedBatchStored(ABatchId))

        outputFetchProtocolState.localOutputMissingBatches should be(empty)
        outputFetchProtocolState.incomingBatchRequests should be(empty)
      }
    }

  "it receives OutputFetch.FetchedBatchStored but the batchId doesn't match" should {
    "not store the batch" in {
      val outputFetchProtocolState = new MainOutputFetchProtocolState()
      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

      val otherBatchId = WrongBatchId
      outputFetchProtocolState.localOutputMissingBatches.addOne(
        otherBatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
      )
      val availability = createAvailability[IgnoringUnitTestEnv](
        outputFetchProtocolState = outputFetchProtocolState,
        availabilityStore = availabilityStore,
      )
      assertLogs(
        availability.receive(
          RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, otherBatchId, ABatch)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include("BatchId doesn't match digest")
        },
      )

      outputFetchProtocolState.localOutputMissingBatches should contain only otherBatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
      outputFetchProtocolState.incomingBatchRequests should be(empty)
      verifyZeroInteractions(availabilityStore)
    }
  }

  "it receives OutputFetch.FetchedBatchStored but there are more requests than allowed" should {
    "not store the batch" in {
      val outputFetchProtocolState = new MainOutputFetchProtocolState()
      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

      outputFetchProtocolState.localOutputMissingBatches.addOne(
        ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
      )
      val availability = createAvailability[IgnoringUnitTestEnv](
        outputFetchProtocolState = outputFetchProtocolState,
        availabilityStore = availabilityStore,
        maxRequestsInBatch = 0,
      )
      assertLogs(
        availability.receive(
          RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, ABatchId, ABatch)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include(
            "Batch BatchId(SHA-256:f9fbd79100fb...) from 'node1' contains more requests (1) than allowed (0), skipping"
          )
        },
      )

      outputFetchProtocolState.localOutputMissingBatches should contain only ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
      outputFetchProtocolState.incomingBatchRequests should be(empty)
      verifyZeroInteractions(availabilityStore)
    }
  }

  "it receives OutputFetch.FetchedBatchStored and " +
    "the batch is missing" should {
      "just remove it from missing" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()

        outputFetchProtocolState.localOutputMissingBatches.addOne(
          ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
        )
        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState
        )
        availability.receive(LocalOutputFetch.FetchedBatchStored(ABatchId))

        outputFetchProtocolState.localOutputMissingBatches should be(empty)
        outputFetchProtocolState.incomingBatchRequests should be(empty)
      }
    }

  "it receives OutputFetch.FetchRemoteBatchDataTimeout and " +
    "the batch is not missing" should {
      "do nothing" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()

        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState
        )
        availability.receive(LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId))

        outputFetchProtocolState.localOutputMissingBatches should be(empty)
        outputFetchProtocolState.incomingBatchRequests should be(empty)
      }
    }

  "it receives OutputFetch.FetchRemoteBatchDataTimeout, " +
    "the batch is missing and " +
    "there are nodes left to try" should {
      "update the fetch progress with the remaining nodes, " +
        "update the missing batches, " +
        "set a fetch timeout and " +
        "send OutputFetch.FetchRemoteBatchData to the current attempted node" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

          outputFetchProtocolState.localOutputMissingBatches.addOne(
            ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
          )
          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAvailability[ProgrammableUnitTestEnv](
            otherNodes = AMissingBatchStatusNode1And2AcksWithNode1ToTry.remainingNodesToTry.toSet,
            outputFetchProtocolState = outputFetchProtocolState,
            cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
          )
          availability.receive(LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId))

          outputFetchProtocolState.localOutputMissingBatches should
            contain only ABatchId -> AMissingBatchStatusNode1And2AcksWithNoAttemptsLeft
          outputFetchProtocolState.incomingBatchRequests should be(empty)
          context.delayedMessages should contain(
            LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId)
          )

          p2pNetworkOutCell.get() shouldBe None
          context.runPipedMessagesAndReceiveOnModule(availability)

          p2pNetworkOutCell.get() should contain(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0).fakeSign
              ),
              Set(Node1),
            )
          )
        }
    }

  "it receives OutputFetch.FetchRemoteBatchDataTimeout, " +
    "the batch is missing and " +
    "there are no nodes left to try" should {
      "restart from the whole proof of availability or the topology, " +
        "update the fetch progress with the remaining nodes, " +
        "update the missing batches, " +
        "set a fetch timeout and " +
        "send OutputFetch.FetchRemoteBatchData to the current attempted node" in {
          forAll(
            Table[MissingBatchStatus, Set[BftNodeId], MissingBatchStatus, BftNodeId](
              (
                "missing batch status",
                "other nodes",
                "new missing batch status",
                "expected send to",
              ),
              (
                AMissingBatchStatusNode1And2AcksWithNoAttemptsLeft,
                Set.from(AMissingBatchStatusNode1And2AcksWithNode1ToTry.remainingNodesToTry),
                AMissingBatchStatusNode1And2AcksWithNode2ToTry,
                Node1,
              ),
              // Ignore nodes from the PoA, use the current topology
              (
                AMissingBatchStatusFromStateTransferWithNoAttemptsLeft,
                Set(Node3),
                AMissingBatchStatusFromStateTransferWithNoAttemptsLeft,
                Node3,
              ),
            )
          ) { (missingBatchStatus, otherNodes, newMissingBatchStatus, expectedSendTo) =>
            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
            val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
            val fetchRemoteBatchData =
              RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0)
            when(
              cryptoProvider.signMessage(
                fetchRemoteBatchData,
                AuthenticatedMessageType.BftSignedAvailabilityMessage,
              )
            ) thenReturn (() => Right(fetchRemoteBatchData.fakeSign))

            outputFetchProtocolState.localOutputMissingBatches.addOne(
              ABatchId -> missingBatchStatus
            )
            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            val availability = createAvailability[ProgrammableUnitTestEnv](
              otherNodes = otherNodes,
              outputFetchProtocolState = outputFetchProtocolState,
              cryptoProvider = cryptoProvider,
              p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            )
            loggerFactory.assertLoggedWarningsAndErrorsSeq(
              availability.receive(LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId)),
              forEvery(_) { entry =>
                entry.message should include("got fetch timeout")
                entry.message should include("no nodes")
                entry.message should include("restarting fetch from the beginning")
              },
            )

            outputFetchProtocolState.localOutputMissingBatches should
              contain only ABatchId -> newMissingBatchStatus
            outputFetchProtocolState.incomingBatchRequests should be(empty)
            context.delayedMessages should contain(
              LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId)
            )

            p2pNetworkOutCell.get() shouldBe None
            context.runPipedMessagesAndReceiveOnModule(availability)

            verify(cryptoProvider).signMessage(
              fetchRemoteBatchData,
              AuthenticatedMessageType.BftSignedAvailabilityMessage,
            )

            p2pNetworkOutCell.get() should contain(
              P2PNetworkOut.Multicast(
                P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                  RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0).fakeSign
                ),
                Set(expectedSendTo),
              )
            )
          }
        }
    }

  "it receives Consensus.CreateProposal (from local consensus) and " +
    "there are no batches ready for ordering" should {
      "record the proposal request" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val mempoolCell = new AtomicReference[Option[Mempool.Message]](None)

        val availability = createAvailability[IgnoringUnitTestEnv](
          disseminationProtocolState = disseminationProtocolState,
          mempool = fakeCellModule(mempoolCell),
        )
        mempoolCell.get() should contain(
          Mempool.CreateLocalBatches(
            (BftBlockOrdererConfig.DefaultMaxBatchesPerProposal * AvailabilityModule.DisseminateAheadMultiplier).toShort
          )
        )
        availability.receive(
          Availability.Consensus
            .CreateProposal(OrderingTopologyNode0, failingCryptoProvider, EpochNumber.First)
        )

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus
        disseminationProtocolState.batchesReadyForOrdering should be(empty)

      }
    }

  "it receives Consensus.CreateProposal (from local consensus) and " +
    "some time has passed with no batches ready for ordering" should {
      "record the proposal request and " +
        "send empty block proposal to local consensus" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val mempoolCell = new AtomicReference[Option[Mempool.Message]](None)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val timerCell = new AtomicReference[
            Option[(DelayCount, Availability.Message[FakeTimerCellUnitTestEnv])]
          ](None)
          implicit val timeCellContext
              : FakeTimerCellUnitTestContext[Availability.Message[FakeTimerCellUnitTestEnv]] =
            new FakeTimerCellUnitTestContext(timerCell)
          val clock = new SimClock(loggerFactory = loggerFactory)

          // initially consensus requests a proposal and there is nothing to be ordered, then a message is sent to mempool
          val availability = createAvailability[FakeTimerCellUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            mempool = fakeCellModule(mempoolCell),
            consensus = fakeCellModule(consensusCell),
            clock = clock,
          )
          mempoolCell.get() should contain(
            Mempool.CreateLocalBatches(
              (BftBlockOrdererConfig.DefaultMaxBatchesPerProposal * AvailabilityModule.DisseminateAheadMultiplier).toShort
            )
          )
          availability.receive(
            Availability.Consensus
              .CreateProposal(OrderingTopologyNode0, failingCryptoProvider, EpochNumber.First)
          )

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus
          disseminationProtocolState.batchesReadyForOrdering should be(empty)

          consensusCell.get() shouldBe None
          timerCell.get() shouldBe None

          // a clock tick signals that we want to check if it's time to propose an empty block,
          // but not enough time has yet passed so nothing happens, only a new clock tick is scheduled
          availability.receive(Availability.Consensus.LocalClockTick)

          consensusCell.get() shouldBe None
          timerCell.get() should contain(1 -> Availability.Consensus.LocalClockTick)

          // after enough time has passed, and we do a new clock tick, we propose an empty block to consensus
          clock.advance(EmptyBlockCreationInterval.plus(1.micro).toJava)
          availability.receive(Availability.Consensus.LocalClockTick)

          consensusCell.get() shouldBe Some(
            ProposalCreated(OrderingBlock(List.empty), EpochNumber.First)
          ) // empty block
          timerCell.get() should contain(2 -> Availability.Consensus.LocalClockTick)
        }
    }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there are more batches ready for ordering than " +
    "requested by local consensus and " +
    "topology is unchanged" should {
      "send a fully-sized proposal to local consensus and " +
        "have batches left ready for ordering" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val pipeToSelfQueue =
            new mutable.Queue[() => Option[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ]]()
          implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
            Availability.Message[FakePipeToSelfQueueUnitTestEnv]
          ] =
            FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

          val numberOfBatchesReadyForOrdering =
            BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt
          val batchesReadyForOrderingRange =
            0 to numberOfBatchesReadyForOrdering // both interval extremes are inclusive, i.e., 1 extra batch
          val batchIds = batchesReadyForOrderingRange
            .map(i => BatchId.createForTesting(s"batch $i"))
          val batchIdsWithMetadata =
            batchesReadyForOrderingRange.map(n =>
              batchIds(n) -> InProgressBatchMetadata(
                batchIds(n),
                anEpochNumber,
                OrderingRequestBatchStats.ForTesting,
              ).complete(
                ProofOfAvailabilityNode0AckNode0InTopology.copy(batchId = batchIds(n)).acks
              )
            )
          disseminationProtocolState.batchesReadyForOrdering.addAll(
            batchIdsWithMetadata
          )
          val mempoolCell = new AtomicReference[Option[Mempool.Message]](None)

          val availability =
            createAvailability[FakePipeToSelfQueueUnitTestEnv](
              disseminationProtocolState = disseminationProtocolState,
              consensus = fakeCellModule(consensusCell),
              mempool = fakeCellModule(mempoolCell),
            )
          mempoolCell.get() should contain(
            Mempool.CreateLocalBatches(
              (BftBlockOrdererConfig.DefaultMaxBatchesPerProposal * AvailabilityModule.DisseminateAheadMultiplier - numberOfBatchesReadyForOrdering - 1).toShort
            )
          )
          availability.receive(
            Availability.Consensus
              .CreateProposal(OrderingTopologyNode0, failingCryptoProvider, EpochNumber.First)
          )

          val batchIdsWithProofsOfAvailabilityReadyForOrdering = batchIdsWithMetadata
            .slice(
              0,
              numberOfBatchesReadyForOrdering,
            )
          val proposedProofsOfAvailability =
            batchIdsWithProofsOfAvailabilityReadyForOrdering.map(_._2)
          consensusCell.get() should matchPattern {
            case Some(
                  Consensus.LocalAvailability
                    .ProposalCreated(
                      OrderingBlock(poas),
                      EpochNumber.First,
                    )
                )
                if poas.toSet.sizeIs == BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt =>
          }
          pipeToSelfQueue shouldBe empty

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)

          availability.receive(
            Availability.Consensus.Ordered(
              proposedProofsOfAvailability.map(_.proofOfAvailability.batchId)
            )
          )
          disseminationProtocolState.batchesReadyForOrdering should
            contain only batchIdsWithMetadata(
              numberOfBatchesReadyForOrdering
            )
          mempoolCell.get() should contain(
            Mempool.CreateLocalBatches(
              (BftBlockOrdererConfig.DefaultMaxBatchesPerProposal * AvailabilityModule.DisseminateAheadMultiplier - 1).toShort
            )
          )
        }

      "return the same response if no ack is given from consensus" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
        val pipeToSelfQueue =
          new mutable.Queue[() => Option[Availability.Message[FakePipeToSelfQueueUnitTestEnv]]]()
        implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
          Availability.Message[FakePipeToSelfQueueUnitTestEnv]
        ] =
          FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

        val numberOfBatchesReadyForOrdering =
          BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt * 2
        val batchIds =
          (0 until numberOfBatchesReadyForOrdering)
            .map(i => BatchId.createForTesting(s"batch $i"))
        val batchIdsWithMetadata =
          (0 until numberOfBatchesReadyForOrdering).map(n =>
            batchIds(n) -> InProgressBatchMetadata(
              batchIds(n),
              anEpochNumber,
              OrderingRequestBatchStats.ForTesting,
            ).complete(ProofOfAvailabilityNode0AckNode0InTopology.copy(batchId = batchIds(n)).acks)
          )
        disseminationProtocolState.batchesReadyForOrdering.addAll(
          batchIdsWithMetadata
        )
        val availability = createAvailability[FakePipeToSelfQueueUnitTestEnv](
          disseminationProtocolState = disseminationProtocolState,
          consensus = fakeCellModule(consensusCell),
        )

        {
          availability.receive(
            Availability.Consensus
              .CreateProposal(OrderingTopologyNode0, failingCryptoProvider, EpochNumber.First)
          )
          consensusCell.get() should matchPattern {
            case Some(
                  Consensus.LocalAvailability
                    .ProposalCreated(
                      OrderingBlock(poas),
                      EpochNumber.First,
                    )
                )
                if poas.toSet.sizeIs == BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt =>
          }
          pipeToSelfQueue shouldBe empty

          val proposedProofsOfAvailability = getPoas(consensusCell)
          consensusCell.set(None)

          // if we ask for a proposal again without acking the previous response, we'll get the same thing again
          availability.receive(
            Availability.Consensus
              .CreateProposal(OrderingTopologyNode0, failingCryptoProvider, EpochNumber.First)
          )
          consensusCell.get() should contain(
            Consensus.LocalAvailability
              .ProposalCreated(
                OrderingBlock(proposedProofsOfAvailability),
                EpochNumber.First,
              )
          )
          pipeToSelfQueue shouldBe empty

          consensusCell.set(None)

          // now we ask for a new proposal, but ack the previous one
          availability.receive(
            Availability.Consensus.CreateProposal(
              OrderingTopologyNode0,
              failingCryptoProvider,
              EpochNumber.First,
              orderedBatchIds = proposedProofsOfAvailability.map(_.batchId),
            )
          )

          pipeToSelfQueue shouldBe empty
        }

        {
          val proposedProofsOfAvailability = getPoas(consensusCell)

          consensusCell.get() should contain(
            Consensus.LocalAvailability
              .ProposalCreated(OrderingBlock(proposedProofsOfAvailability), EpochNumber.First)
          )

          availability.receive(
            Availability.Consensus.Ordered(proposedProofsOfAvailability.map(_.batchId))
          )
        }

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
      }
    }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there are less batches ready for ordering than " +
    "requested by local consensus and " +
    "topology is unchanged" should {
      "send a non-empty but not fully-sized proposal to local consensus and " +
        "have no batches left ready for ordering" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val pipeToSelfQueue =
            new mutable.Queue[() => Option[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ]]()
          implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
            Availability.Message[FakePipeToSelfQueueUnitTestEnv]
          ] =
            FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

          val numberOfBatchesReadyForOrdering =
            BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt - 2
          val batchIds =
            (0 until numberOfBatchesReadyForOrdering)
              .map(i => BatchId.createForTesting(s"batch $i"))
          val batchIdsWithMetadata =
            (0 until numberOfBatchesReadyForOrdering).map(n =>
              batchIds(n) -> InProgressBatchMetadata(
                batchIds(n),
                anEpochNumber,
                OrderingRequestBatchStats.ForTesting,
              ).complete(
                ProofOfAvailabilityNode0AckNode0InTopology.copy(batchId = batchIds(n)).acks
              )
            )
          disseminationProtocolState.batchesReadyForOrdering.addAll(
            batchIdsWithMetadata
          )
          val availability = createAvailability[FakePipeToSelfQueueUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            consensus = fakeCellModule(consensusCell),
          )
          availability.receive(
            Availability.Consensus
              .CreateProposal(OrderingTopologyNode0, failingCryptoProvider, EpochNumber.First)
          )

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)
          disseminationProtocolState.batchesReadyForOrdering should not be empty

          val proposedProofsOfAvailability =
            batchIdsWithMetadata.map(_._2).map(_.proofOfAvailability)
          consensusCell.get() should matchPattern {
            case Some(
                  Consensus.LocalAvailability
                    .ProposalCreated(OrderingBlock(poas), EpochNumber.First)
                ) if poas.toSet.sizeIs == numberOfBatchesReadyForOrdering =>
          }
          pipeToSelfQueue shouldBe empty

          availability.receive(
            Availability.Consensus.Ordered(proposedProofsOfAvailability.map(_.batchId))
          )
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
        }
    }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there are no batches ready for ordering but " +
    "topology becomes strictly smaller and " +
    "an in-progress batch is ready in the new topology" should {
      "move the in-progress batch to ready and " +
        "send it in a proposal to local consensus" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]]()
          val pipeToSelfQueue =
            new mutable.Queue[() => Option[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ]]()
          implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
            Availability.Message[FakePipeToSelfQueueUnitTestEnv]
          ] =
            FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0To6WithNonQuorumVotes
          )
          val availability = createAvailability[FakePipeToSelfQueueUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            consensus = fakeCellModule(consensusCell),
          )
          availability.receive(
            Availability.Consensus
              .CreateProposal(OrderingTopologyNodes0To3, failingCryptoProvider, EpochNumber.First)
          )

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)
          disseminationProtocolState.batchesReadyForOrdering should not be empty

          val proposedProofsOfAvailability = ADisseminationProgressNode0To6WithNonQuorumVotes
            .copy(orderingTopology = OrderingTopologyNodes0To3)
            .proofOfAvailability()

          val poa = proposedProofsOfAvailability.getOrElse(
            fail("PoA should be ready in new topology but isn't")
          )
          consensusCell.get() should contain(
            Consensus.LocalAvailability.ProposalCreated(OrderingBlock(Seq(poa)), EpochNumber.First)
          )
          pipeToSelfQueue shouldBe empty

          availability.receive(Availability.Consensus.Ordered(Seq(poa.batchId)))
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
        }
    }

  "it receives Consensus.CreateProposal (from local consensus) and " +
    "there is a batch ready for ordering but " +
    "the topology becomes strictly bigger and " +
    "the batch is not ready for ordering in the new topology" should {
      "complete batch dissemination" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
        val pipeToSelfQueue =
          new mutable.Queue[() => Option[Availability.Message[FakePipeToSelfQueueUnitTestEnv]]]()
        implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
          Availability.Message[FakePipeToSelfQueueUnitTestEnv]
        ] =
          FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

        disseminationProtocolState.batchesReadyForOrdering.addOne(
          BatchReadyForOrderingNode0Vote
        )
        val availabilityStore = new FakeAvailabilityStore[FakePipeToSelfQueueUnitTestEnv](
          mutable.Map[BatchId, OrderingRequestBatch](
            ABatchId -> ABatch
          )
        )
        val availability = createAvailability[FakePipeToSelfQueueUnitTestEnv](
          disseminationProtocolState = disseminationProtocolState,
          availabilityStore = availabilityStore,
          consensus = fakeCellModule(consensusCell),
        )
        availability.receive(
          Availability.Consensus
            .CreateProposal(OrderingTopologyWithNode0To6, failingCryptoProvider, EpochNumber.First)
        )

        val reviewedProgress =
          DisseminationProgress
            .reviewReadyForOrdering(BatchReadyForOrderingNode0Vote._2, OrderingTopologyWithNode0To6)
        disseminationProtocolState.disseminationProgress should contain only (ABatchId -> reviewedProgress)
        disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus
        disseminationProtocolState.batchesReadyForOrdering should be(empty)

        consensusCell.get() should be(empty)

        val selfSendMessages = pipeToSelfQueue.flatMap(_.apply())
        selfSendMessages should contain only
          Availability.LocalDissemination.LocalBatchesStoredSigned(
            Seq(LocalBatchStoredSigned(ABatchId, ABatch, Left(reviewedProgress)))
          )
      }
    }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there are multiple pending pulls from consensus and" +
    "there are batches ready for ordering," +
    "there are batches in progress but " +
    "the topology becomes strictly smaller; after that " +
    "some in-progress batches are ready in the new topology and " +
    "some batches ready for ordering are not ready anymore in the new topology" should {
      "move the previously in-progress and now completed batches to ready, " +
        "propose them to local consensus and " +
        "complete dissemination of the batches previously ready for ordering that are not ready anymore" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val consensusBuffer = new ArrayBuffer[Consensus.Message[FakePipeToSelfQueueUnitTestEnv]]()
          val pipeToSelfQueue =
            new mutable.Queue[() => Option[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ]]()
          implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
            Availability.Message[FakePipeToSelfQueueUnitTestEnv]
          ] =
            FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

          // This in-progress batch will become ready in the new topology
          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0To6WithNode0And1Votes
          )
          // This ready batch will become stale in the new topology
          disseminationProtocolState.batchesReadyForOrdering.addOne(
            AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes
          )
          // We need local consensus pulls for both the in-progress and ready batches, to ensure that
          //  the ready batch that becomes stale is not included in a proposal to consensus even
          //  if there is one pending.
          disseminationProtocolState.toBeProvidedToConsensus.enqueue(
            ToBeProvidedToConsensus(1, EpochNumber.First)
          )
          disseminationProtocolState.toBeProvidedToConsensus.enqueue(
            ToBeProvidedToConsensus(1, EpochNumber.First)
          )
          val availabilityStore = new FakeAvailabilityStore[FakePipeToSelfQueueUnitTestEnv](
            mutable.Map[BatchId, OrderingRequestBatch](
              AnotherBatchId -> ABatch
            )
          )
          val availability = createAvailability[FakePipeToSelfQueueUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            availabilityStore = availabilityStore,
            consensus = fakeRecordingModule(consensusBuffer),
          )
          availability.receive(
            Availability.Consensus
              .CreateProposal(OrderingTopologyNodes0To3, failingCryptoProvider, EpochNumber.First)
          )

          val reviewedProgress =
            DisseminationProgress
              .reviewReadyForOrdering(
                AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes._2,
                OrderingTopologyNodes0To3,
              )
          disseminationProtocolState.disseminationProgress should contain only (AnotherBatchId -> reviewedProgress)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)
          disseminationProtocolState.batchesReadyForOrdering.keys should contain only ABatchId

          val proposedProofsOfAvailability = ADisseminationProgressNode0To6WithNonQuorumVotes
            .copy(orderingTopology = OrderingTopologyNodes0To3)
            .proofOfAvailability()

          val poa = proposedProofsOfAvailability.getOrElse(
            fail("PoA should be ready in new topology but isn't")
          )
          consensusBuffer should contain only
            Consensus.LocalAvailability.ProposalCreated(OrderingBlock(Seq(poa)), EpochNumber.First)

          val selfMessages = pipeToSelfQueue.flatMap(_.apply())
          selfMessages should contain only Availability.LocalDissemination
            .LocalBatchesStoredSigned(
              Seq(LocalBatchStoredSigned(AnotherBatchId, ABatch, Left(reviewedProgress)))
            )
        }
    }

  // TODO(#24403): add tests about key rotations

  "it receives " +
    "Dissemination.StoreLocalBatch, " +
    "Dissemination.StoreRemoteBatch " +
    "and OutputFetch.StoreFetchedBatch" should {

      type Msg = Availability.Message[ProgrammableUnitTestEnv]

      "store the batch" in {
        forAll(
          Table[Msg, Msg](
            ("message", "reply"),
            (
              Availability.LocalDissemination.LocalBatchCreated(Seq(anOrderingRequest)),
              Availability.LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)),
            ),
            (
              Availability.RemoteDissemination.RemoteBatch.create(
                ABatchId,
                ABatch,
                Node0,
              ),
              Availability.LocalDissemination
                .RemoteBatchStored(ABatchId, anEpochNumber, Node0),
            ),
            (
              Availability.RemoteOutputFetch.RemoteBatchDataFetched.create(
                Node0,
                ABatchId,
                ABatch,
              ),
              Availability.LocalOutputFetch.FetchedBatchStored(ABatchId),
            ),
          )
        ) { (message, reply) =>
          val outputFetchProtocolState = new MainOutputFetchProtocolState()
          outputFetchProtocolState.localOutputMissingBatches.addOne(
            ABatchId -> MissingBatchStatus(
              ABatchId,
              ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
              Seq(Node1),
              mode = OrderedBlockForOutput.Mode.FromConsensus,
            )
          )
          val storage = mutable.Map[BatchId, OrderingRequestBatch]()
          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAvailability[ProgrammableUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState,
            availabilityStore = new FakeAvailabilityStore[ProgrammableUnitTestEnv](storage),
          )

          availability.receive(message)

          storage shouldBe empty
          context.runPipedMessages() shouldBe Seq(reply)
          storage should contain only (ABatchId -> ABatch)
        }
      }

      "LocalBatchStored and RemoteBatchStored should be signed" in {
        forAll(
          Table[Msg, Msg](
            ("message", "reply"),
            (
              Availability.LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)),
              Availability.LocalDissemination
                .LocalBatchesStoredSigned(
                  Seq(LocalBatchStoredSigned(ABatchId, ABatch, Right(Signature.noSignature)))
                ),
            ),
            (
              Availability.LocalDissemination
                .RemoteBatchStored(ABatchId, anEpochNumber, Node0),
              Availability.LocalDissemination
                .RemoteBatchStoredSigned(ABatchId, Node0, Signature.noSignature),
            ),
          )
        ) { case (message, reply) =>
          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)
          val availability = createAvailability[ProgrammableUnitTestEnv](
            cryptoProvider = cryptoProvider
          )

          availability.receive(message)

          context.runPipedMessages() shouldBe Seq(reply)
          verify(cryptoProvider).signHash(
            AvailabilityAck.hashFor(ABatchId, anEpochNumber, Node0)
          )
        }
      }

      "after having stored a remote batch, " +
        "update the pending requests and fetch it " +
        "when it's the only one missing" in {
          forAll(
            Table[Msg](
              "message",
              Availability.LocalDissemination
                .RemoteBatchStoredSigned(ABatchId, Node0, Signature.noSignature),
              Availability.LocalOutputFetch.FetchedBatchStored(ABatchId),
            )
          ) { message =>
            val singleBatchMissingRequest = new BatchesRequest(
              AnOrderedBlockForOutput,
              missingBatches = mutable.SortedSet(ABatchId),
            )

            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            outputFetchProtocolState.pendingBatchesRequests.addOne(singleBatchMissingRequest)
            outputFetchProtocolState.localOutputMissingBatches.addOne(
              ABatchId -> MissingBatchStatus(
                ABatchId,
                ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
                Seq(Node1),
                mode = OrderedBlockForOutput.Mode.FromConsensus,
              )
            )
            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext

            val availabilityStore = spy(new FakeAvailabilityStore[ProgrammableUnitTestEnv]())
            val availability = createAvailability[ProgrammableUnitTestEnv](
              availabilityStore = availabilityStore,
              outputFetchProtocolState = outputFetchProtocolState,
              cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            )
            val result = mock[AvailabilityStore.FetchBatchesResult]
            when(availabilityStore.fetchBatches(Seq(ABatchId))) thenReturn (() => result)

            availability.receive(message)

            outputFetchProtocolState.pendingBatchesRequests shouldBe empty
            singleBatchMissingRequest.missingBatches shouldBe empty
            context.runPipedMessages() should contain(
              LocalOutputFetch.FetchedBlockDataFromStorage(singleBatchMissingRequest, result)
            )
          }
        }

      "after having stored a remote batch, " +
        "update the pending requests and " +
        "don't fetch it when batches are still missing" in {
          forAll(
            Table[Msg](
              "message",
              Availability.LocalDissemination
                .RemoteBatchStoredSigned(ABatchId, Node0, Signature.noSignature),
              Availability.LocalOutputFetch.FetchedBatchStored(ABatchId),
            )
          ) { message =>
            val multipleBatchMissingRequest = new BatchesRequest(
              AnotherOrderedBlockForOutput,
              missingBatches = mutable.SortedSet(ABatchId, AnotherBatchId),
            )

            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            outputFetchProtocolState.pendingBatchesRequests.addOne(multipleBatchMissingRequest)
            outputFetchProtocolState.localOutputMissingBatches.addOne(
              ABatchId -> MissingBatchStatus(
                ABatchId,
                ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
                Seq(Node1),
                mode = OrderedBlockForOutput.Mode.FromConsensus,
              )
            )
            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext

            val availabilityStore = spy(new FakeAvailabilityStore[ProgrammableUnitTestEnv]())
            val availability = createAvailability[ProgrammableUnitTestEnv](
              availabilityStore = availabilityStore,
              outputFetchProtocolState = outputFetchProtocolState,
              cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            )

            availability.receive(message)

            outputFetchProtocolState.pendingBatchesRequests.size should be(1)
            outputFetchProtocolState
              .pendingBatchesRequests(0)
              .missingBatches should contain only AnotherBatchId
            context.runPipedMessages()
            multipleBatchMissingRequest.missingBatches.toSet shouldBe Set(AnotherBatchId)
            verifyZeroInteractions(availabilityStore)
          }
        }
    }

  "it receives OutputFetch.FetchedBlockDataFromStorage and there are no missing batches" should {
    "send to output" in {
      val storage = mutable.Map[BatchId, OrderingRequestBatch]()
      storage.addOne(ABatchId -> ABatch)
      val availabilityStore = spy(new FakeAvailabilityStore[FakePipeToSelfCellUnitTestEnv](storage))
      val cellContextFake =
        new AtomicReference[
          Option[() => Option[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
        ](None)
      val expectedOutputCell =
        new AtomicReference[Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]](None)
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Availability.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cellContextFake)
      val availability = createAvailability(
        availabilityStore = availabilityStore,
        output = fakeCellModule(expectedOutputCell),
      )
      val request = new BatchesRequest(AnOrderedBlockForOutput, mutable.SortedSet(ABatchId))

      availability.receive(
        Availability.LocalOutputFetch.FetchedBlockDataFromStorage(
          request,
          AvailabilityStore.AllBatches(Seq(ABatchId -> ABatch)),
        )
      )

      expectedOutputCell.get() shouldBe Some(
        Output.BlockDataFetched(ACompleteBlock)
      )
    }
  }

  "it receives OutputFetch.FetchedBlockDataFromStorage and there are missing batches" should {
    "record the missing batches and ask other node for missing data" in {
      forAll(
        Table[OrderedBlockForOutput.Mode, BftNodeId](
          ("block mode", "expected send to"),
          (OrderedBlockForOutput.Mode.FromConsensus, Node1),
          // Ignore nodes from the PoA, use the current topology
          (OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock, Node3),
          // Ignore nodes from the PoA, use the current topology
          (OrderedBlockForOutput.Mode.StateTransfer.LastBlock, Node3),
        )
      ) { (blockMode, expectedSendTo) =>
        val outputFetchProtocolState = new MainOutputFetchProtocolState()
        val expectedMessageCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
        val cellNetwork = fakeCellModule(expectedMessageCell)
        val availabilityStore = spy(new FakeAvailabilityStore[ProgrammableUnitTestEnv])
        implicit val context
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext
        val availability = createAvailability(
          otherNodes = Set(Node3),
          availabilityStore = availabilityStore,
          outputFetchProtocolState = outputFetchProtocolState,
          cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
          p2pNetworkOut = cellNetwork,
        )

        val request = new BatchesRequest(
          OrderedBlockForOutput(
            OrderedBlock(
              ABlockMetadata,
              Seq(ProofOfAvailabilityNode1And2AcksNode1And2InTopology),
              CanonicalCommitSet(Set.empty),
            ),
            ViewNumber.First,
            isLastInEpoch = false, // Irrelevant for availability
            from = Node0,
            mode = blockMode,
          ),
          mutable.SortedSet(ABatchId),
        )
        outputFetchProtocolState.pendingBatchesRequests.addOne(request)

        availability.receive(
          Availability.LocalOutputFetch
            .FetchedBlockDataFromStorage(
              request,
              AvailabilityStore.MissingBatches(Set(ABatchId)),
            )
        )

        outputFetchProtocolState.pendingBatchesRequests.size should be(1)
        outputFetchProtocolState
          .pendingBatchesRequests(0)
          .missingBatches should contain only ABatchId

        expectedMessageCell.get() shouldBe None

        context.runPipedMessagesAndReceiveOnModule(availability)

        expectedMessageCell.get() should contain(
          P2PNetworkOut.send(
            P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
              Availability.RemoteOutputFetch.FetchRemoteBatchData
                .create(ABatchId, from = Node0)
                .fakeSign
            ),
            expectedSendTo,
          )
        )
      }
    }
  }

  "it receives OutputFetch.LoadBatchData and the batch is present" should {
    "reply to local availability with the batch" in {
      val storage = mutable.Map[BatchId, OrderingRequestBatch]()
      storage.addOne(ABatchId -> ABatch)
      val cellContextFake =
        new AtomicReference[
          Option[() => Option[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
        ](None)
      val availabilityStore = spy(new FakeAvailabilityStore[FakePipeToSelfCellUnitTestEnv](storage))
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Availability.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cellContextFake)
      val availability = createAvailability(
        availabilityStore = availabilityStore
      )

      availability.receive(
        Availability.RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0)
      )

      cellContextFake.get() shouldBe defined
      cellContextFake.get().foreach { f =>
        f() shouldBe Some(
          Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, Some(ABatch))
        )
      }
    }
  }

  "it receives OutputFetch.LoadBatchData and the batch is missing" should {
    "reply to local availability without the batch" in {
      val cellContextFake =
        new AtomicReference[
          Option[() => Option[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
        ](None)
      val availabilityStore = spy(new FakeAvailabilityStore[FakePipeToSelfCellUnitTestEnv])
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Availability.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cellContextFake)
      val availability = createAvailability(
        availabilityStore = availabilityStore
      )

      availability.receive(
        Availability.RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0)
      )

      cellContextFake.get() shouldBe defined
      cellContextFake.get().foreach { f =>
        f() shouldBe
          Some(Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, None))
      }
    }
  }

  "it receives UnverifiedProtocolMessage should verify it" should {
    "if okay should send underlying to self" in {
      implicit val context
          : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext
      val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
      val availability = createAvailability[ProgrammableUnitTestEnv](
        cryptoProvider = cryptoProvider
      )

      val underlyingMessage = mock[Availability.RemoteProtocolMessage]
      val signedMessage = underlyingMessage.fakeSign

      when(
        cryptoProvider.verifySignedMessage(
          signedMessage,
          AuthenticatedMessageType.BftSignedAvailabilityMessage,
        )
      ) thenReturn (() => Either.unit)

      availability.receive(Availability.UnverifiedProtocolMessage(signedMessage))

      context.runPipedMessages() shouldBe Seq(underlyingMessage)
    }

    "if not okay should drop" in {

      implicit val context
          : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext
      val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
      val availability = createAvailability[ProgrammableUnitTestEnv](
        cryptoProvider = cryptoProvider
      )

      val underlyingMessage = mock[Availability.RemoteProtocolMessage]
      val signedMessage = underlyingMessage.fakeSign
      val signatureCheckError = mock[SignatureCheckError]

      when(underlyingMessage.from) thenReturn Node1

      when(
        cryptoProvider.verifySignedMessage(
          signedMessage,
          AuthenticatedMessageType.BftSignedAvailabilityMessage,
        )
      ) thenReturn (() => Left(signatureCheckError))

      availability.receive(Availability.UnverifiedProtocolMessage(signedMessage))

      assertLogs(
        context.runPipedMessages() shouldBe Seq(Availability.NoOp),
        (logEntry: LogEntry) => {
          logEntry.level shouldBe Level.INFO
          logEntry.message should include("Skipping message since we can't verify signature")
        },
      )
    }
  }

  private class FakeAvailabilityStore[E <: BaseIgnoringUnitTestEnv[E]](
      storage: mutable.Map[BatchId, OrderingRequestBatch] = mutable.Map.empty
  ) extends GenericInMemoryAvailabilityStore[E](storage) {
    override def createFuture[A](action: String)(x: () => Try[A]): () => A = () => x().success.value
    override def close(): Unit = ()
  }

  private def createAvailability[E <: BaseIgnoringUnitTestEnv[E]](
      myId: BftNodeId = Node0,
      otherNodes: Set[BftNodeId] = Set.empty,
      maxRequestsInBatch: Short = BftBlockOrdererConfig.DefaultMaxRequestsInBatch,
      maxBatchesPerProposal: Short = BftBlockOrdererConfig.DefaultMaxBatchesPerProposal,
      mempool: ModuleRef[Mempool.Message] = fakeIgnoringModule,
      cryptoProvider: CryptoProvider[E] = failingCryptoProvider[E],
      availabilityStore: data.AvailabilityStore[E] = new FakeAvailabilityStore[E],
      clock: Clock = new SimClock(loggerFactory = loggerFactory),
      output: ModuleRef[Output.Message[E]] = fakeModuleExpectingSilence,
      consensus: ModuleRef[Consensus.Message[E]] = fakeModuleExpectingSilence,
      p2pNetworkOut: ModuleRef[P2PNetworkOut.Message] = fakeIgnoringModule,
      disseminationProtocolState: DisseminationProtocolState = new DisseminationProtocolState(),
      outputFetchProtocolState: MainOutputFetchProtocolState = new MainOutputFetchProtocolState(),
  )(implicit context: E#ActorContextT[Availability.Message[E]]): AvailabilityModule[E] = {
    val config = AvailabilityModuleConfig(
      maxRequestsInBatch,
      maxBatchesPerProposal,
      BftBlockOrdererConfig.DefaultOutputFetchTimeout,
    )
    val dependencies = AvailabilityModuleDependencies[E](
      mempool,
      p2pNetworkOut,
      consensus,
      output,
    )
    val availability = new AvailabilityModule[E](
      Membership.forTesting(myId, otherNodes),
      cryptoProvider,
      availabilityStore,
      config,
      clock,
      new Random(0),
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      dependencies,
      loggerFactory,
      timeouts,
      disseminationProtocolState,
      outputFetchProtocolState,
    )(MetricsContext.Empty)
    availability.receive(Availability.Start)
    availability
  }

  private def remoteBatchAcknowledged(idx: Int) =
    RemoteDissemination.RemoteBatchAcknowledged.create(
      ABatchId,
      from = node(idx),
      Signature.noSignature,
    )

  private def newMockCrypto = {
    val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
    when(cryptoProvider.signHash(any[Hash])(any[TraceContext])).thenReturn(() =>
      Right(Signature.noSignature)
    )
    cryptoProvider
  }

  def getPoas(
      consensusCell: AtomicReference[Option[Consensus.ProtocolMessage]]
  ): Seq[ProofOfAvailability] =
    consensusCell
      .get()
      .getOrElse(fail())
      .asInstanceOf[ProposalCreated]
      .orderingBlock
      .proofs

  private def node(n: Int): BftNodeId =
    BftNodeId(s"node$n")
}
