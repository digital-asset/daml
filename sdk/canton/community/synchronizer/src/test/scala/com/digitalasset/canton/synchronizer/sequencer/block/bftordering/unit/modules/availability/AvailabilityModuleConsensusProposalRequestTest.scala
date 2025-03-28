// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.availability

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModuleConfig.EmptyBlockCreationInterval
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftKeyId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  OrderingRequestBatch,
  OrderingRequestBatchStats,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.LocalDissemination.LocalBatchStoredSigned
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.LocalAvailability.ProposalCreated
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.UnitTestContext.DelayCount
import com.digitalasset.canton.time.SimClock
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

class AvailabilityModuleConsensusProposalRequestTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  "The availability module" when {

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
          "send an empty block proposal to local consensus" in {
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
      "the new topology has a smaller weak quorum and " +
      "an in-progress batch already has a new weak quorum" should {

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
              Consensus.LocalAvailability
                .ProposalCreated(OrderingBlock(Seq(poa)), EpochNumber.First)
            )
            pipeToSelfQueue shouldBe empty

            availability.receive(Availability.Consensus.Ordered(Seq(poa.batchId)))
            disseminationProtocolState.batchesReadyForOrdering should be(empty)
          }
      }

    "it receives Consensus.CreateProposal (from local consensus) and " +
      "there is a batch ready for ordering but " +
      "the new topology has a bigger weak quorum and " +
      "the batch that was ready for ordering doesn't have a new weak quorum" should {

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
            TrieMap[BatchId, OrderingRequestBatch](ABatchId -> ABatch)
          )
          val availability = createAvailability[FakePipeToSelfQueueUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            availabilityStore = availabilityStore,
            consensus = fakeCellModule(consensusCell),
          )
          availability.receive(
            Availability.Consensus
              .CreateProposal(
                OrderingTopologyWithNode0To6,
                failingCryptoProvider,
                EpochNumber.First,
              )
          )

          val reviewedProgress =
            DisseminationProgress
              .reviewReadyForOrdering(
                BatchReadyForOrderingNode0Vote._2,
                OrderingTopologyWithNode0To6,
              )
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
      "there is a batch ready for ordering," +
      "there is a batch in progress but " +
      "the new topology has a smaller weak quorum; after that " +
      "the in-progress batch has a new weak quorum and " +
      "the batch that was ready for ordering doesn't have a new weak quorum" should {

        "move the previously in-progress batch to ready, " +
          "move the previously ready batch to in-progress, " +
          "propose only the ready batch to local consensus and " +
          "complete dissemination of the in-progress batch" in {
            val disseminationProtocolState = new DisseminationProtocolState()
            val consensusBuffer =
              new ArrayBuffer[Consensus.Message[FakePipeToSelfQueueUnitTestEnv]]()
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
              TrieMap[BatchId, OrderingRequestBatch](AnotherBatchId -> ABatch)
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
              Consensus.LocalAvailability.ProposalCreated(
                OrderingBlock(Seq(poa)),
                EpochNumber.First,
              )

            val selfMessages = pipeToSelfQueue.flatMap(_.apply())
            selfMessages should contain only Availability.LocalDissemination
              .LocalBatchesStoredSigned(
                Seq(LocalBatchStoredSigned(AnotherBatchId, ABatch, Left(reviewedProgress)))
              )
          }
      }
  }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there is a batch ready for ordering but " +
    "the new topology has the same size but different keys and " +
    "invalidates one of its acks from other nodes" should {

      "move the previously ready batch to in-progress and " +
        "complete dissemination of the in-progress batch" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val consensusBuffer =
            new ArrayBuffer[Consensus.Message[FakePipeToSelfQueueUnitTestEnv]]()
          val pipeToSelfQueue =
            new mutable.Queue[() => Option[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ]]()
          implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
            Availability.Message[FakePipeToSelfQueueUnitTestEnv]
          ] =
            FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

          // This ready batch will become stale in the new topology
          disseminationProtocolState.batchesReadyForOrdering.addOne(
            AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes
          )

          val availabilityStore = new FakeAvailabilityStore[FakePipeToSelfQueueUnitTestEnv](
            TrieMap[BatchId, OrderingRequestBatch](
              AnotherBatchId -> ABatch
            )
          )
          val availability = createAvailability[FakePipeToSelfQueueUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            availabilityStore = availabilityStore,
            consensus = fakeRecordingModule(consensusBuffer),
          )
          val newTopology =
            OrderingTopologyNodes0To6.copy(
              nodesTopologyInfo =
                OrderingTopologyNodes0To6.nodesTopologyInfo.map { case (nodeId, nodeInfo) =>
                  // Change the key of node5 and node6 so that the PoA is only left with 2 valid acks < f+1 = 3
                  nodeId -> (if (nodeId == "node5" || nodeId == "node6")
                               nodeInfo.copy(keyIds =
                                 Set(BftKeyId(anotherNoSignature.signedBy.toProtoPrimitive))
                               )
                             else nodeInfo)
                }
            )
          availability.receive(
            Availability.Consensus
              .CreateProposal(
                newTopology,
                failingCryptoProvider,
                EpochNumber.First,
              )
          )

          val reviewedProgress =
            DisseminationProgress
              .reviewReadyForOrdering(
                AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes._2,
                newTopology,
              )

          disseminationProtocolState.disseminationProgress should contain only (AnotherBatchId -> reviewedProgress)
          disseminationProtocolState.toBeProvidedToConsensus should contain only
            ToBeProvidedToConsensus(16, EpochNumber.First)
          disseminationProtocolState.batchesReadyForOrdering shouldBe empty
          consensusBuffer shouldBe empty
          val selfMessages = pipeToSelfQueue.flatMap(_.apply())
          selfMessages should contain only Availability.LocalDissemination
            .LocalBatchesStoredSigned(
              Seq(LocalBatchStoredSigned(AnotherBatchId, ABatch, Left(reviewedProgress)))
            )
        }
    }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there is a batch ready for ordering but " +
    "the new topology has the same size but different keys and " +
    "invalidates the ack from the disseminating node" should {

      "move the previously ready batch to in-progress, " +
        "sign the batch again and " +
        "re-disseminate the in-progress batch" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val consensusBuffer =
            new ArrayBuffer[Consensus.Message[FakePipeToSelfQueueUnitTestEnv]]()
          val pipeToSelfQueue =
            new mutable.Queue[() => Option[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ]]()
          implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
            Availability.Message[FakePipeToSelfQueueUnitTestEnv]
          ] =
            FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

          // This ready batch will become stale in the new topology
          disseminationProtocolState.batchesReadyForOrdering.addOne(
            AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes
          )

          val availabilityStore = new FakeAvailabilityStore[FakePipeToSelfQueueUnitTestEnv](
            TrieMap[BatchId, OrderingRequestBatch](
              AnotherBatchId -> ABatch
            )
          )
          val availability = createAvailability[FakePipeToSelfQueueUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            availabilityStore = availabilityStore,
            consensus = fakeRecordingModule(consensusBuffer),
          )
          val newTopology =
            OrderingTopologyNodes0To6.copy(
              nodesTopologyInfo =
                OrderingTopologyNodes0To6.nodesTopologyInfo.map { case (nodeId, nodeInfo) =>
                  // Change the key of node0 so that the batch has to be re-signed and re-disseminated
                  nodeId -> (if (nodeId == "node0")
                               nodeInfo.copy(keyIds =
                                 Set(BftKeyId(anotherNoSignature.signedBy.toProtoPrimitive))
                               )
                             else nodeInfo)
                }
            )
          availability.receive(
            Availability.Consensus
              .CreateProposal(
                newTopology,
                failingCryptoProvider,
                EpochNumber.First,
              )
          )

          disseminationProtocolState.disseminationProgress shouldBe empty
          disseminationProtocolState.toBeProvidedToConsensus should contain only
            ToBeProvidedToConsensus(16, EpochNumber.First)
          disseminationProtocolState.batchesReadyForOrdering shouldBe empty
          consensusBuffer shouldBe empty
          val selfMessages = pipeToSelfQueue.flatMap(_.apply())
          selfMessages should contain only Availability.LocalDissemination
            .LocalBatchesStored(
              Seq(AnotherBatchId -> ABatch)
            )
        }
    }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there are expired batches ready for ordering" should {

      "only propose non-expired batches and remove the expired batches from ready for ordering " in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
        val initialEpochNumber = EpochNumber(OrderingRequestBatch.BatchValidityDurationEpochs + 1L)
        val availability = createAvailability[IgnoringUnitTestEnv](
          consensus = fakeCellModule(consensusCell),
          disseminationProtocolState = disseminationProtocolState,
          initialEpochNumber = EpochNumber(initialEpochNumber - 1L),
        )

        val (validBatchIds, expiredBatchIds) = {
          val numberOfBatchesReadyForOrdering =
            BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt
          val batchIds =
            (0 until numberOfBatchesReadyForOrdering).map(i =>
              BatchId.createForTesting(s"batch $i")
            )
          (
            batchIds.take(numberOfBatchesReadyForOrdering / 2),
            batchIds.drop(numberOfBatchesReadyForOrdering / 2),
          )
        }

        {
          def batchIdWithMetadata(batchId: BatchId, epochNumber: EpochNumber) =
            batchId -> InProgressBatchMetadata(
              batchId,
              epochNumber,
              OrderingRequestBatchStats.ForTesting,
            ).complete(
              ProofOfAvailabilityNode0AckNode0InTopology.copy(batchId = batchId).acks
            )
          val validBatchIdsWithMetadata =
            validBatchIds.map(batchId => batchIdWithMetadata(batchId, initialEpochNumber))
          val expiredBatchIdsWithMetadata =
            expiredBatchIds.map(batchId => batchIdWithMetadata(batchId, EpochNumber.First))
          disseminationProtocolState.batchesReadyForOrdering.addAll(
            validBatchIdsWithMetadata ++ expiredBatchIdsWithMetadata
          )
        }

        loggerFactory.assertLogs(
          availability.receive(
            Availability.Consensus
              .CreateProposal(OrderingTopologyNode0, failingCryptoProvider, initialEpochNumber)
          ),
          log => {
            log.level shouldBe Level.WARN
            log.message should include regex (
              s"Discarding the batches with the following ids .* because they are expired"
            )
          },
        )

        inside(consensusCell.get()) {
          case Some(
                Consensus.LocalAvailability
                  .ProposalCreated(
                    OrderingBlock(poas),
                    `initialEpochNumber`,
                  )
              ) =>
            poas.map(_.batchId) should contain theSameElementsAs validBatchIds
        }

        disseminationProtocolState.batchesReadyForOrdering.keys should contain theSameElementsAs validBatchIds
      }
    }
}
