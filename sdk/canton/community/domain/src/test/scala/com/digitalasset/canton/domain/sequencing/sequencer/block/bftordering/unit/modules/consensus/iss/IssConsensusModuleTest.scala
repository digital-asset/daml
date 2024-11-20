// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultEpochLength
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpoch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.GenericInMemoryEpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.leaders.SimpleLeaderSelectionPolicy
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferManager
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlocksReader
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.memory.GenericInMemoryOutputBlockMetadataStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  TopologyActivationTime,
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
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.snapshot.{
  PeerActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Consensus.NewEpochTopology
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  ConsensusSegment,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.mockito.Mockito
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level.ERROR

import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class IssConsensusModuleTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  import IssConsensusModuleTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  private val blockOrder4Nodes =
    Iterator.continually(allPeers).flatten.take(DefaultEpochLength.toInt).toSeq
  private val blockMetadata4Nodes = blockOrder4Nodes.zipWithIndex.map { case (_, blockNum) =>
    BlockMetadata.mk(EpochNumber.First, blockNum.toLong)
  }

  "IssConsensusModule" when {

    "not yet started via explicit signal" should {
      "queue incoming messages and not produce any outgoing messages" in {
        val (context, consensus) = createIssConsensusModule()
        implicit val ctx: ContextType = context
        consensus.receive(
          Consensus.LocalAvailability.ProposalCreated(oneRequestOrderingBlock, EpochNumber.First)
        )
        // verifies that no ModuleRef receives any messages from Consensus
        succeed
      }
    }

    "a new ordering topology is received" should {

      "start a new epoch when it hasn't been started" in {
        val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
        val latestCompletedEpochFromStore = EpochStore.Epoch(
          EpochInfo(
            EpochNumber.First,
            BlockNumber.First,
            epochLength,
            TopologyActivationTime(aTimestamp),
          ),
          Seq.empty,
        )
        when(epochStore.latestEpoch(anyBoolean)(any[TraceContext])).thenReturn(() =>
          latestCompletedEpochFromStore
        )
        when(epochStore.startEpoch(latestCompletedEpochFromStore.info)).thenReturn(() => ())

        val (context, consensus) =
          createIssConsensusModule(
            epochStore = epochStore,
            preConfiguredInitialEpochState = Some(newEpochState(latestCompletedEpochFromStore, _)),
          )
        implicit val ctx: ContextType = context

        consensus.receive(Consensus.Start)
        consensus.receive(
          Consensus.NewEpochTopology(
            EpochNumber(1L),
            OrderingTopology(allPeers.toSet),
            fakeCryptoProvider,
          )
        )

        verify(epochStore).startEpoch(latestCompletedEpochFromStore.info.next(epochLength))
        succeed
      }

      "start segment modules only once when state transfer is completed" in {
        val segmentModuleMock = mock[ModuleRef[ConsensusSegment.Message]]
        val stateTransferManagerMock = mock[StateTransferManager[ProgrammableUnitTestEnv]]
        when(stateTransferManagerMock.isInStateTransfer).thenReturn(true)

        val membership = Membership(selfId, otherPeers.toSet)
        val aStartEpoch = GenesisEpoch.info.next(epochLength)

        val (context, consensus) =
          createIssConsensusModule(
            p2pNetworkOutModuleRef = fakeIgnoringModule,
            otherPeers = membership.otherPeers,
            segmentModuleFactoryFunction = () => segmentModuleMock,
            stateTransferManagerOpt = Some(stateTransferManagerMock),
          )
        implicit val ctx: ContextType = context

        consensus.receive(
          Consensus.NewEpochStored(
            aStartEpoch.next(epochLength), // not important for the test
            membership,
            fakeCryptoProvider,
          )
        )

        val order = Mockito.inOrder(stateTransferManagerMock, segmentModuleMock)
        order.verify(stateTransferManagerMock).clearStateTransferState()
        order
          .verify(segmentModuleMock, times(membership.orderingTopology.peers.size))
          .asyncSend(ConsensusSegment.Start)
        succeed
      }

      "do nothing if a new epoch is already in progress" in {
        val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
        val latestCompletedEpochFromStore = EpochStore.Epoch(
          EpochInfo(
            EpochNumber.First,
            BlockNumber.First,
            epochLength,
            TopologyActivationTime(aTimestamp),
          ),
          Seq.empty,
        )
        when(epochStore.latestEpoch(includeInProgress = false)).thenReturn(() =>
          latestCompletedEpochFromStore
        )
        when(epochStore.latestEpoch(includeInProgress = true)).thenReturn(() =>
          EpochStore.Epoch(
            latestCompletedEpochFromStore.info.next(epochLength),
            Seq.empty,
          )
        )
        val activeStartingEpochInfo = latestCompletedEpochFromStore.info.next(epochLength)
        when(epochStore.loadEpochProgress(activeStartingEpochInfo)).thenReturn(() =>
          EpochStore.EpochInProgress(
            Seq.empty,
            Seq.empty,
          )
        )
        when(epochStore.startEpoch(latestCompletedEpochFromStore.info)).thenReturn(() => ())

        val (context, consensus) =
          createIssConsensusModule(epochStore = epochStore)
        implicit val ctx: ContextType = context

        consensus.receive(Consensus.Start)
        consensus.receive(
          Consensus.NewEpochTopology(
            EpochNumber(1L),
            OrderingTopology(allPeers.toSet),
            fakeCryptoProvider,
          )
        )

        verify(epochStore, never).startEpoch(any[EpochInfo])(any[TraceContext])
        succeed
      }

      "abort if the current epoch state is behind the last completed epoch" in {
        val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
        val latestCompletedEpochFromStore = EpochStore.Epoch(
          EpochInfo(
            EpochNumber.First,
            BlockNumber.First,
            epochLength,
            TopologyActivationTime(aTimestamp),
          ),
          Seq.empty,
        )
        when(epochStore.latestEpoch(anyBoolean)(any[TraceContext])).thenReturn(() =>
          latestCompletedEpochFromStore
        )
        when(epochStore.startEpoch(latestCompletedEpochFromStore.info)).thenReturn(() => ())

        val (context, consensus) =
          createIssConsensusModule(
            epochStore = epochStore,
            preConfiguredInitialEpochState = Some(
              newEpochState(
                EpochStore.Epoch(
                  GenesisEpoch.info,
                  Seq.empty,
                ),
                _,
              )
            ),
          )
        implicit val ctx: ContextType = context

        consensus.receive(Consensus.Start)

        assertThrows[TestFailedException](
          loggerFactory.assertLogs(
            consensus.receive(
              Consensus.NewEpochTopology(
                EpochNumber(1L),
                OrderingTopology(allPeers.toSet),
                fakeCryptoProvider,
              )
            ),
            log => {
              log.level shouldBe ERROR
              log.message should include("the current epoch number is neither")
            },
          )
        )
      }
    }

    "started via explicit signal" should {

      "self-send the next epoch's topology if starting from genesis" in {
        val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
        val latestCompletedEpochFromStore =
          EpochStore.Epoch(
            GenesisEpoch.info,
            Seq.empty,
          )
        when(epochStore.latestEpoch(anyBoolean)(any[TraceContext])).thenReturn(() =>
          latestCompletedEpochFromStore
        )
        val (context, consensus) =
          createIssConsensusModule(
            epochStore = epochStore,
            preConfiguredInitialEpochState = Some(
              newEpochState(
                latestCompletedEpochFromStore,
                _,
              )
            ),
            segmentModuleFactoryFunction = () => fakeModuleExpectingSilence,
          )
        implicit val ctx: ContextType = context

        consensus.getEpochState.isEpochComplete shouldBe true
        consensus.receive(Consensus.Start)

        context.extractSelfMessages() should matchPattern {
          case Seq(
                Consensus.NewEpochTopology(
                  epochNumber,
                  orderingTopology,
                  _,
                )
              )
              if epochNumber == EpochNumber.First && orderingTopology == OrderingTopology(
                Set(selfId)
              ) =>
        }
      }

      "do nothing (waiting for the next epoch's topology) if the current epoch is complete" in {
        val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
        val latestCompletedEpochFromStore = EpochStore.Epoch(
          EpochInfo(
            EpochNumber.First,
            BlockNumber.First,
            EpochLength(0),
            TopologyActivationTime(aTimestamp),
          ),
          Seq.empty,
        ) // Has length 0, so it's complete (and it's not the genesis)
        when(epochStore.latestEpoch(anyBoolean)(any[TraceContext])).thenReturn(() =>
          latestCompletedEpochFromStore
        )
        when(epochStore.loadEpochProgress(latestCompletedEpochFromStore.info)).thenReturn(() =>
          EpochStore.EpochInProgress(Seq.empty, Seq.empty)
        )
        val (context, consensus) =
          createIssConsensusModule(
            epochStore = epochStore,
            segmentModuleFactoryFunction = () => fakeModuleExpectingSilence,
          )
        implicit val ctx: ContextType = context

        consensus.getEpochState.isEpochComplete shouldBe true
        consensus.receive(Consensus.Start)

        consensus.getEpochState.epoch.info shouldBe latestCompletedEpochFromStore.info
        context.runPipedMessages() shouldBe empty
        context.extractSelfMessages() shouldBe empty
      }

      "complete the epoch when all blocks from all segments complete" in {
        val outputBuffer =
          new ArrayBuffer[Output.Message[ProgrammableUnitTestEnv]](defaultBufferSize)
        val epochLength = DefaultEpochLength

        val (context, consensus) = createIssConsensusModule(
          outputModuleRef = fakeRecordingModule(outputBuffer),
          otherPeers = otherPeers.toSet,
          epochLength = epochLength,
        )
        implicit val ctx: ContextType = context

        consensus.receive(Consensus.Start)

        // Consensus is starting from genesis, so it'll start a new epoch
        val newEpochTopologyMsg = NewEpochTopology(
          EpochNumber.First,
          OrderingTopology(allPeers.toSet),
          fakeCryptoProvider,
        )
        val selfSentMessages = context.extractSelfMessages()
        selfSentMessages should matchPattern {
          case Seq(
                Consensus.NewEpochTopology(
                  epochNumber,
                  orderingTopology,
                  _,
                )
              )
              if epochNumber == newEpochTopologyMsg.epochNumber &&
                orderingTopology == newEpochTopologyMsg.orderingTopology =>
        }
        selfSentMessages.foreach(consensus.receive)
        // Store the new epoch and update the epoch state
        context.runPipedMessagesAndReceiveOnModule(consensus)

        // One by one, complete blocks to finish all segments in the epoch
        (0 until epochLength.toInt).foreach { n =>
          val leaderOfBlock = blockOrder4Nodes(n)
          val isLastBlockInEpoch = n == epochLength - 1
          val expectedOrderedBlock = orderedBlockFromPrePrepare(
            PrePrepare.create(
              blockMetadata4Nodes(n),
              ViewNumber.First,
              clock.now,
              OrderingBlock(oneRequestOrderingBlock.proofs),
              CanonicalCommitSet(Set.empty),
              leaderOfBlock,
            )
          )

          consensus.receive(
            Consensus.ConsensusMessage.BlockOrdered(expectedOrderedBlock, Seq.empty)
          )
          outputBuffer should contain theSameElementsInOrderAs Seq[
            Output.Message[FakePipeToSelfCellUnitTestEnv]
          ](
            Output.BlockOrdered(
              OrderedBlockForOutput(
                expectedOrderedBlock,
                leaderOfBlock,
                isLastBlockInEpoch,
                OrderedBlockForOutput.Mode.FromConsensus,
              )
            )
          )
          outputBuffer.clear()

          if (isLastBlockInEpoch) {
            context.runPipedMessages() should matchPattern {
              case Seq(Consensus.ConsensusMessage.CompleteEpochStored(_)) =>
            }
          } else {
            context.extractSelfMessages() shouldBe empty
          }
        }

        succeed
      }

      "start state transfer when a snapshot is provided" in {
        val stateTransferManagerMock = mock[StateTransferManager[ProgrammableUnitTestEnv]]
        val segmentModuleMock = mock[ModuleRef[ConsensusSegment.Message]]

        val membership = Membership(selfId, otherPeers.toSet)
        val aStartEpoch = GenesisEpoch.info.next(epochLength)

        val (context, consensus) =
          createIssConsensusModule(
            p2pNetworkOutModuleRef = fakeIgnoringModule,
            otherPeers = membership.otherPeers,
            // Trigger state transfer for onboarding
            sequencerSnapshotAdditionalInfo = Some(
              SequencerSnapshotAdditionalInfo(
                Map(
                  selfId -> PeerActiveAt(
                    Some(TopologyActivationTime(CantonTimestamp.MinValue)),
                    Some(aStartEpoch.number),
                    Some(aStartEpoch.startBlockNumber),
                    previousBftTime = None,
                  )
                )
              )
            ),
            segmentModuleFactoryFunction = () => segmentModuleMock,
            stateTransferManagerOpt = Some(stateTransferManagerMock),
          )
        implicit val ctx: ContextType = context

        consensus.receive(Consensus.Start)

        verify(stateTransferManagerMock).startStateTransfer(
          eqTo(membership),
          eqTo(GenesisEpoch),
          eqTo(aStartEpoch.number),
        )(any[String => Nothing])(any[ContextType], eqTo(traceContext))
        // Should not yet start segment modules
        verify(segmentModuleMock, never).asyncSend(ConsensusSegment.Start)
        succeed
      }
    }
  }

  private def newEpochState(
      latestCompletedEpochFromStore: EpochStore.Epoch,
      context: ContextType,
      segmentModuleFactoryFunction: () => ModuleRef[ConsensusSegment.Message] = () =>
        fakeIgnoringModule,
  ): EpochState[ProgrammableUnitTestEnv] = {
    val epochStateEpoch =
      EpochState.Epoch(
        latestCompletedEpochFromStore.info,
        membership = Membership(selfId),
        SimpleLeaderSelectionPolicy,
      )
    new EpochState(
      epoch = epochStateEpoch,
      clock,
      fail(_),
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      createSegmentModuleRefFactory(segmentModuleFactoryFunction)(
        context,
        epochStateEpoch,
        fakeCryptoProvider,
        Seq.empty,
        EpochStore.EpochInProgress(),
      ),
      Seq.empty,
      loggerFactory,
      timeouts,
    )
  }

  private def createIssConsensusModule(
      availabilityModuleRef: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]] =
        fakeModuleExpectingSilence,
      outputModuleRef: ModuleRef[Output.Message[ProgrammableUnitTestEnv]] =
        fakeModuleExpectingSilence,
      p2pNetworkOutModuleRef: ModuleRef[P2PNetworkOut.Message] = fakeModuleExpectingSilence,
      epochLength: EpochLength = DefaultEpochLength,
      otherPeers: Set[SequencerId] = Set.empty,
      epochStore: EpochStore[ProgrammableUnitTestEnv] =
        new InMemoryUnitTestEpochStore[ProgrammableUnitTestEnv],
      outputBlocksReader: OutputBlocksReader[ProgrammableUnitTestEnv] =
        new InMemoryUnitTestOutputBlockMetadataStore[ProgrammableUnitTestEnv],
      preConfiguredInitialEpochState: Option[
        ContextType => EpochState[ProgrammableUnitTestEnv]
      ] = None,
      sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo] = None,
      segmentModuleFactoryFunction: () => ModuleRef[ConsensusSegment.Message] = () =>
        fakeIgnoringModule,
      stateTransferManagerOpt: Option[StateTransferManager[ProgrammableUnitTestEnv]] = None,
  ): (ContextType, IssConsensusModule[ProgrammableUnitTestEnv]) = {
    implicit val context: ContextType = new ProgrammableUnitTestContext

    val dependencies = ConsensusModuleDependencies[ProgrammableUnitTestEnv](
      availabilityModuleRef,
      outputModuleRef,
      p2pNetworkOutModuleRef,
    )

    val initialMembership = Membership(selfId, otherPeers = otherPeers)

    val latestCompletedEpochFromStore =
      epochStore.latestEpoch(includeInProgress = false)(TraceContext.empty)()

    val latestEpochFromStore =
      epochStore.latestEpoch(includeInProgress = true)(TraceContext.empty)()

    val initialEpochState =
      preConfiguredInitialEpochState
        .map(_(context))
        .getOrElse(
          PreIssConsensusModule.initialEpochState(
            initialMembership,
            fakeCryptoProvider,
            clock,
            fail(_),
            latestEpochFromStore,
            epochStore.loadEpochProgress(latestEpochFromStore.info)(TraceContext.empty)(),
            SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
            loggerFactory,
            timeouts,
            createSegmentModuleRefFactory(segmentModuleFactoryFunction),
          )(MetricsContext.Empty, context)
        )

    val stateTransferManager = stateTransferManagerOpt.getOrElse(
      new StateTransferManager(
        dependencies,
        epochLength,
        epochStore,
        outputBlocksReader,
        selfId,
        loggerFactory,
      )
    )

    context -> new IssConsensusModule(
      epochLength = epochLength,
      IssConsensusModule.StartupState(
        sequencerSnapshotAdditionalInfo,
        initialMembership,
        fakeCryptoProvider,
        initialEpochState,
        latestCompletedEpochFromStore,
      ),
      epochStore,
      outputBlocksReader,
      clock,
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      createSegmentModuleRefFactory(segmentModuleFactoryFunction),
      selfId,
      dependencies,
      loggerFactory,
      timeouts,
    )(stateTransferManager)(MetricsContext.Empty)
  }
}

object IssConsensusModuleTest {

  private type ContextType =
    ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]]
  private val epochLength = DefaultEpochLength
  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))
  private val defaultBufferSize = 5
  private val selfId = fakeSequencerId("self")
  private val otherPeers: IndexedSeq[SequencerId] = (1 to 3).map { index =>
    fakeSequencerId(
      s"peer$index"
    )
  }
  private val allPeers = (selfId +: otherPeers).sorted
  private val aBatchId = BatchId.createForTesting("A batch id")
  private val oneRequestOrderingBlock = OrderingBlock(Seq(ProofOfAvailability(aBatchId, Seq.empty)))

  private def createSegmentModuleRefFactory(
      segmentModuleFactoryFunction: () => ModuleRef[ConsensusSegment.Message] = () =>
        fakeIgnoringModule
  ) =
    new SegmentModuleRefFactory[ProgrammableUnitTestEnv] {
      override def apply(
          _context: ContextType,
          epoch: EpochState.Epoch,
          cryptoProvider: CryptoProvider[ProgrammableUnitTestEnv],
          latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
          epochInProgress: EpochStore.EpochInProgress,
      )(
          segmentState: SegmentState,
          metricsAccumulator: EpochMetricsAccumulator,
      ): ModuleRef[ConsensusSegment.Message] = segmentModuleFactoryFunction()
    }

  def orderedBlockFromPrePrepare(prePrepare: PrePrepare): OrderedBlock =
    OrderedBlock(
      prePrepare.blockMetadata,
      prePrepare.block.proofs,
      prePrepare.canonicalCommitSet,
    )
}

final class InMemoryUnitTestEpochStore[E <: BaseIgnoringUnitTestEnv[E]]
    extends GenericInMemoryEpochStore[E] {
  override protected def createFuture[T](action: String)(
      value: () => Try[T]
  ): E#FutureUnlessShutdownT[T] = () => value().get
}

final class InMemoryUnitTestOutputBlockMetadataStore[E <: BaseIgnoringUnitTestEnv[E]]
    extends GenericInMemoryOutputBlockMetadataStore[E] {
  override protected def createFuture[T](action: String)(
      value: () => Try[T]
  ): E#FutureUnlessShutdownT[T] = () => value().get
}
