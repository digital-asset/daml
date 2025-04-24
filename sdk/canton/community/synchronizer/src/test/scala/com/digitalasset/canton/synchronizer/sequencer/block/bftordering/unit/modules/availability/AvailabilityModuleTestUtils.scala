// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.availability

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.{Fingerprint, Hash, Signature, SignatureFormat}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule.quorum
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.memory.GenericInMemoryAvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.{
  AvailabilityModule,
  AvailabilityModuleConfig,
  DisseminationProgress,
  DisseminationProtocolState,
  MainOutputFetchProtocolState,
  MissingBatchStatus,
  ToBeProvidedToConsensus,
  data,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftKeyId,
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.NodeTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  MessageAuthorizer,
  OrderingTopology,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequest,
  OrderingRequestBatch,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.RemoteDissemination
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.LocalAvailability.ProposalCreated
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.AvailabilityModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Mempool,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.{
  BaseIgnoringUnitTestEnv,
  IgnoringUnitTestContext,
  IgnoringUnitTestEnv,
  ProgrammableUnitTestEnv,
  failingCryptoProvider,
  fakeIgnoringModule,
  fakeModuleExpectingSilence,
}
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.util.{Random, Try}

private[availability] trait AvailabilityModuleTestUtils { self: BftSequencerBaseTest =>

  protected val Node0 = node(0)
  protected val Node1 = node(1)
  protected val Node2 = node(2)
  protected val Node3 = node(3)
  protected val Node1And2 = (1 to 2).map(node).toSet
  protected val Node0To3 = (0 to 3).map(node).toSet
  protected val Node1To3 = (1 to 3).map(node).toSet
  protected val Node1To6 = (1 to 6).map(node).toSet
  protected val AnotherBatchId = BatchId.createForTesting("AnotherBatchId")
  protected val anEpochNumber = EpochNumber.First
  protected val anOrderingRequest: Traced[OrderingRequest] = Traced(
    OrderingRequest("tag", ByteString.EMPTY)
  )
  protected val ABatch = OrderingRequestBatch.create(
    Seq(anOrderingRequest),
    anEpochNumber,
  )
  protected val ABatchId = BatchId.from(ABatch)
  protected val AnInProgressBatchMetadata =
    InProgressBatchMetadata(ABatchId, anEpochNumber, ABatch.stats)
  protected val WrongBatchId = BatchId.createForTesting("Wrong BatchId")
  protected val ABlockMetadata: BlockMetadata =
    BlockMetadata.mk(
      epochNumber = 0,
      blockNumber = 0,
    )
  protected val AnOrderedBlockForOutput = OrderedBlockForOutput(
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
  protected val AnotherOrderedBlockForOutput = OrderedBlockForOutput(
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
  protected val ACompleteBlock = CompleteBlockData(
    AnOrderedBlockForOutput,
    Seq(ABatchId -> ABatch),
  )
  protected val Node0Ack = AvailabilityAck(from = Node0, Signature.noSignature)
  protected val Node0Acks = Set(Node0Ack)
  protected val Node0And1Acks = Seq(
    AvailabilityAck(from = Node0, Signature.noSignature),
    AvailabilityAck(from = Node1, Signature.noSignature),
  )
  protected val FirstFourNodesQuorumAcks = (0 until quorum(numberOfNodes = 4)).map { idx =>
    AvailabilityAck(from = node(idx), Signature.noSignature)
  }
  protected val Nodes0And4To6Acks =
    (Range.inclusive(0, 0) ++ (4 until 7)).map { idx =>
      AvailabilityAck(from = node(idx), Signature.noSignature)
    }
  protected val anotherNoSignature =
    Signature.create(
      SignatureFormat.Symbolic,
      ByteString.EMPTY,
      Fingerprint.tryFromString("another-no-fingerprint"),
      None,
    )
  protected val Node1And2Acks = Seq(
    AvailabilityAck(from = Node1, Signature.noSignature),
    AvailabilityAck(from = Node2, Signature.noSignature),
  )
  protected val OrderingTopologyNode0 = OrderingTopology.forTesting(Set(Node0))
  protected val ADisseminationProgressNode0WithNode0Vote =
    DisseminationProgress(
      OrderingTopologyNode0,
      AnInProgressBatchMetadata,
      acks = Node0Acks,
    )
  protected val OrderingTopologyNodes0And1 = OrderingTopology.forTesting(Set(Node0, Node1))

  protected val ADisseminationProgressNode0And1WithNode0Vote =
    ADisseminationProgressNode0WithNode0Vote.copy(
      orderingTopology = OrderingTopologyNodes0And1
    )
  protected val ADisseminationProgressNode0And1WithNode0And1Votes =
    DisseminationProgress(
      OrderingTopologyNodes0And1,
      AnInProgressBatchMetadata,
      acks = Node0And1Acks.toSet,
    )
  protected val OrderingTopologyNodes0To3 = OrderingTopology.forTesting(Node0To3)
  protected val ADisseminationProgressNode0To3WithNode0Vote =
    ADisseminationProgressNode0WithNode0Vote.copy(
      orderingTopology = OrderingTopologyNodes0To3
    )
  protected val OrderingTopologyWithNode0To6 = OrderingTopology.forTesting(Node1To6 + Node0)
  protected val ADisseminationProgressNode0To6WithNode0Vote =
    ADisseminationProgressNode0WithNode0Vote.copy(
      orderingTopology = OrderingTopologyWithNode0To6
    )
  protected val ADisseminationProgressNode0To6WithNode0And1Vote =
    ADisseminationProgressNode0And1WithNode0And1Votes.copy(
      orderingTopology = OrderingTopologyWithNode0To6
    )
  protected val QuorumAcksForNode0To3 =
    (0 until quorum(numberOfNodes = 4)).map { idx =>
      remoteBatchAcknowledged(idx)
    }
  protected val NonQuorumAcksForNode0To6 =
    (0 until quorum(numberOfNodes = 7) - 1).map { idx =>
      remoteBatchAcknowledged(idx)
    }
  protected val ADisseminationProgressNode0To6WithNonQuorumVotes =
    ADisseminationProgressNode0To6WithNode0Vote.copy(
      acks = (0 until quorum(numberOfNodes = 7) - 1).map { idx =>
        AvailabilityAck(from = node(idx), Signature.noSignature)
      }.toSet
    )
  protected val ABatchDisseminationProgressNode0And1WithNode0Vote =
    ABatchId -> ADisseminationProgressNode0And1WithNode0Vote
  protected val ABatchDisseminationProgressNode0To3WithNode0Vote =
    ABatchId -> ADisseminationProgressNode0To3WithNode0Vote
  protected val ABatchDisseminationProgressNode0To6WithNode0Vote =
    ABatchId -> ADisseminationProgressNode0To6WithNode0Vote
  protected val ABatchDisseminationProgressNode0To6WithNode0And1Votes =
    ABatchId -> ADisseminationProgressNode0To6WithNode0And1Vote
  protected val ABatchDisseminationProgressNode0To6WithNonQuorumVotes =
    ABatchId -> ADisseminationProgressNode0To6WithNonQuorumVotes
  protected val ProofOfAvailabilityNode0AckNode0InTopology = ProofOfAvailability(
    ABatchId,
    Node0Acks.toSeq,
    anEpochNumber,
  )
  protected val ProofOfAvailabilityNode0AckNode0To2InTopology = ProofOfAvailability(
    ABatchId,
    Node0Acks.toSeq,
    anEpochNumber,
  )
  protected val ProofOfAvailabilityNode1And2AcksNode1And2InTopology = ProofOfAvailability(
    ABatchId,
    Node1And2Acks,
    anEpochNumber,
  )
  protected val BatchReadyForOrderingNode0Vote =
    ABatchId -> InProgressBatchMetadata(
      ABatchId,
      anEpochNumber,
      ABatch.stats,
    ).complete(ProofOfAvailabilityNode0AckNode0InTopology.acks)
  protected val ABatchProposalNode0VoteNode0InTopology =
    Consensus.LocalAvailability.ProposalCreated(
      OrderingBlock(
        Seq(ProofOfAvailabilityNode0AckNode0InTopology)
      ),
      EpochNumber.First,
    )
  protected val ABatchProposalNode0VoteNodes0To2InTopology =
    Consensus.LocalAvailability.ProposalCreated(
      OrderingBlock(
        Seq(ProofOfAvailabilityNode0AckNode0To2InTopology)
      ),
      EpochNumber.First,
    )
  protected val ProofOfAvailabilityNode0And1VotesNodes0And1InTopology = ProofOfAvailability(
    ABatchId,
    Node0And1Acks,
    anEpochNumber,
  )
  protected val ProofOfAvailability4NodesQuorumVotesNodes0To3InTopology = ProofOfAvailability(
    ABatchId,
    FirstFourNodesQuorumAcks,
    anEpochNumber,
  )
  protected val ProofOfAvailability6NodesQuorumVotesNodes0And4To6InTopology = ProofOfAvailability(
    ABatchId,
    Nodes0And4To6Acks,
    anEpochNumber,
  )
  protected val BatchReadyForOrderingNode0And1Votes =
    ABatchId -> InProgressBatchMetadata(ABatchId, anEpochNumber, ABatch.stats)
      .complete(ProofOfAvailabilityNode0And1VotesNodes0And1InTopology.acks)
  protected val BatchReadyForOrdering4NodesQuorumVotes =
    ABatchId -> InProgressBatchMetadata(ABatchId, anEpochNumber, ABatch.stats)
      .complete(ProofOfAvailability4NodesQuorumVotesNodes0To3InTopology.acks)
  protected val AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes =
    AnotherBatchId -> InProgressBatchMetadata(AnotherBatchId, anEpochNumber, ABatch.stats)
      .complete(ProofOfAvailability6NodesQuorumVotesNodes0And4To6InTopology.acks)
  protected val ABatchProposalNode0And1Votes = Consensus.LocalAvailability.ProposalCreated(
    OrderingBlock(
      Seq(ProofOfAvailabilityNode0And1VotesNodes0And1InTopology)
    ),
    EpochNumber.First,
  )
  protected val ABatchProposal4NodesQuorumVotes = Consensus.LocalAvailability.ProposalCreated(
    OrderingBlock(
      Seq(ProofOfAvailability4NodesQuorumVotesNodes0To3InTopology)
    ),
    EpochNumber.First,
  )
  protected val AMissingBatchStatusNode1And2AcksWithNode1ToTry =
    MissingBatchStatus(
      ABatchId,
      ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
      remainingNodesToTry = Seq(Node1),
      mode = OrderedBlockForOutput.Mode.FromConsensus,
    )
  protected val AMissingBatchStatusNode1And2AcksWithNode2ToTry =
    AMissingBatchStatusNode1And2AcksWithNode1ToTry
      .copy(remainingNodesToTry =
        ProofOfAvailabilityNode1And2AcksNode1And2InTopology.acks.map(_.from).tail
      )
  protected val AMissingBatchStatusNode1And2AcksWithNoAttemptsLeft =
    AMissingBatchStatusNode1And2AcksWithNode1ToTry
      .copy(remainingNodesToTry = Seq.empty)
  protected val ABatchMissingBatchStatusNode1And2AcksWithNoAttemptsLeft =
    ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
  protected val AMissingBatchStatusFromStateTransferWithNoAttemptsLeft =
    AMissingBatchStatusNode1And2AcksWithNoAttemptsLeft
      .copy(mode = OrderedBlockForOutput.Mode.FromStateTransfer)
  protected val AToBeProvidedToConsensus =
    ToBeProvidedToConsensus(
      BftBlockOrdererConfig.DefaultMaxBatchesPerProposal,
      EpochNumber.First,
    )
  protected val Node0To6 = (0 to 6).map(node).toSet
  protected val OrderingTopologyNodes0To6 = OrderingTopology.forTesting(Node0To6)

  protected implicit val fakeTimerIgnoringUnitTestContext
      : IgnoringUnitTestContext[Availability.Message[IgnoringUnitTestEnv]] =
    IgnoringUnitTestContext()

  protected class FakeAvailabilityStore[E <: BaseIgnoringUnitTestEnv[E]](
      storage: TrieMap[BatchId, OrderingRequestBatch] = TrieMap.empty
  ) extends GenericInMemoryAvailabilityStore[E](storage) {
    override def createFuture[A](action: String)(x: () => Try[A]): () => A = () => x().success.value
    override def close(): Unit = ()
  }

  protected def createAvailability[E <: BaseIgnoringUnitTestEnv[E]](
      myId: BftNodeId = Node0,
      otherNodes: Set[BftNodeId] = Set.empty,
      otherNodesCustomKeys: Map[BftNodeId, BftKeyId] = Map.empty,
      initialEpochNumber: EpochNumber = EpochNumber.First,
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
      customMessageAuthorizer: Option[MessageAuthorizer] = None,
  )(implicit
      synchronizerProtocolVersion: ProtocolVersion,
      context: E#ActorContextT[Availability.Message[E]],
  ): AvailabilityModule[E] = {
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
    val membership = Membership.forTesting(
      myId,
      otherNodes,
      nodesTopologyInfos = otherNodesCustomKeys.map { case (nodeId, keyId) =>
        nodeId -> NodeTopologyInfo(TopologyActivationTime(CantonTimestamp.MinValue), Set(keyId))
      },
    )
    val availability = new AvailabilityModule[E](
      membership,
      initialEpochNumber,
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
    )(customMessageAuthorizer.getOrElse(membership.orderingTopology))(
      synchronizerProtocolVersion,
      MetricsContext.Empty,
    )
    availability.receive(Availability.Start)
    availability
  }

  protected def remoteBatchAcknowledged(idx: Int): RemoteDissemination.RemoteBatchAcknowledged =
    RemoteDissemination.RemoteBatchAcknowledged.create(
      ABatchId,
      from = node(idx),
      Signature.noSignature,
    )

  protected def newMockCrypto: CryptoProvider[ProgrammableUnitTestEnv] = {
    val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
    when(cryptoProvider.signHash(any[Hash])(any[TraceContext])).thenReturn(() =>
      Right(Signature.noSignature)
    )
    cryptoProvider
  }

  protected def getPoas(
      consensusCell: AtomicReference[Option[Consensus.ProtocolMessage]]
  ): Seq[ProofOfAvailability] =
    consensusCell
      .get()
      .getOrElse(fail())
      .asInstanceOf[ProposalCreated]
      .orderingBlock
      .proofs

  protected def node(n: Int): BftNodeId =
    BftNodeId(s"node$n")
}
