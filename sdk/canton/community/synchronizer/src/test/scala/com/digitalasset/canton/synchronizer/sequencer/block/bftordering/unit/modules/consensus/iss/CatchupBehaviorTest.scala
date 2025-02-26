// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.EpochInProgress
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.leaders.SimpleLeaderSelectionPolicy
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.*
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.BlocksStored
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PbftNetworkMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.IssConsensusModuleTest.selfId
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable

class CatchupBehaviorTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  import CatchupBehaviorTest.*
  import IssConsensusModuleTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  "CatchupBehavior" when {

    "ready" should {
      "init" in {
        val (context, catchupBehavior) = createCatchupBehavior()

        catchupBehavior.ready(context.self)

        context.extractSelfMessages() shouldBe Seq(Consensus.Init)
      }
    }

    "init" should {
      "cancel the current epoch" in {
        val epochStateMock = mock[EpochState[ProgrammableUnitTestEnv]]
        when(epochStateMock.epoch) thenReturn anEpoch
        val (context, catchupBehavior) =
          createCatchupBehavior(preConfiguredInitialEpochState = Some(_ => epochStateMock))
        implicit val ctx: ContextType = context

        catchupBehavior.receive(Consensus.Init)

        verify(epochStateMock, times(1)).notifyEpochCancellationToSegments(EpochNumber(1))

        succeed
      }
    }

    "all segments confirm cancelling the epoch" should {
      "start state transfer" in {
        val epochStateMock = mock[EpochState[ProgrammableUnitTestEnv]]
        when(epochStateMock.epoch) thenReturn anEpoch
        val stateTransferManagerMock = mock[StateTransferManager[ProgrammableUnitTestEnv]]
        when(stateTransferManagerMock.inStateTransfer) thenReturn false
        val epochStoreMock = mock[EpochStore[ProgrammableUnitTestEnv]]
        when(
          epochStoreMock.latestEpoch(includeInProgress = eqTo(false))(any[TraceContext])
        ) thenReturn (() => anEpochStoreEpoch)
        val (context, catchupBehavior) =
          createCatchupBehavior(
            preConfiguredInitialEpochState = Some(_ => epochStateMock),
            maybeOnboardingStateTransferManager = Some(stateTransferManagerMock),
            epochStore = epochStoreMock,
          )
        implicit val ctx: ContextType = context

        catchupBehavior.receive(Consensus.CatchUpMessage.SegmentCancelledEpoch)

        verify(stateTransferManagerMock, never).startStateTransfer(
          any[Membership],
          any[CryptoProvider[ProgrammableUnitTestEnv]],
          any[EpochStore.Epoch],
          any[EpochNumber],
        )(any[String => Nothing])(any[ContextType], any[TraceContext])

        catchupBehavior.receive(Consensus.CatchUpMessage.SegmentCancelledEpoch)

        verify(stateTransferManagerMock, times(1)).startStateTransfer(
          eqTo(anEpoch.currentMembership),
          any[CryptoProvider[ProgrammableUnitTestEnv]],
          eqTo(anEpochStoreEpoch),
          eqTo(anEpoch.info.number),
        )(any[String => Nothing])(any[ContextType], any[TraceContext])

        succeed
      }
    }

    "receiving a state transfer message" should {
      "hand it to the state transfer manager" in {
        val stateTransferManagerMock = mock[StateTransferManager[ProgrammableUnitTestEnv]]
        val epochStoreMock = mock[EpochStore[ProgrammableUnitTestEnv]]
        when(
          epochStoreMock.latestEpoch(any[Boolean])(any[TraceContext])
        ) thenReturn (() => anEpochStoreEpoch)
        when(
          epochStoreMock.loadEpochProgress(eqTo(anEpochStoreEpoch.info))(any[TraceContext])
        ) thenReturn (() => EpochInProgress())
        when(
          stateTransferManagerMock.handleStateTransferMessage(
            any[Consensus.StateTransferMessage],
            any[OrderingTopologyInfo[ProgrammableUnitTestEnv]],
            any[EpochStore.Epoch],
          )(any[String => Nothing])(any[ContextType], any[TraceContext])
        ) thenReturn StateTransferMessageResult.Continue
        val (context, catchupBehavior) =
          createCatchupBehavior(
            maybeOnboardingStateTransferManager = Some(stateTransferManagerMock),
            epochStore = epochStoreMock,
          )
        implicit val ctx: ContextType = context

        val aStateTransferMessage = BlocksStored(Seq.empty, EpochNumber.First)
        catchupBehavior.receive(aStateTransferMessage)

        verify(stateTransferManagerMock, times(1)).handleStateTransferMessage(
          eqTo(aStateTransferMessage),
          eqTo(aTopologyInfo),
          eqTo(anEpochStoreEpoch),
        )(any[String => Nothing])(any[ContextType], any[TraceContext])

        succeed
      }
    }

    "handling a 'Continue' result of processing a state transfer message" should {
      "do nothing" in {
        val (context, catchupBehavior) = createCatchupBehavior()
        implicit val ctx: ContextType = context

        catchupBehavior.handleStasteTransferMessageResult(
          "aMessageType",
          StateTransferMessageResult.Continue,
        )

        context.runPipedMessages() shouldBe empty
        context.extractSelfMessages() shouldBe empty
        context.extractBecomes() shouldBe empty
        context.delayedMessages shouldBe empty
      }
    }

    "handling a 'Complete' result of processing a state transfer message" should {
      "transition back to consensus mode" in {
        val (context, catchupBehavior) = createCatchupBehavior()
        implicit val ctx: ContextType = context

        catchupBehavior.handleStasteTransferMessageResult(
          "aMessageType",
          StateTransferMessageResult.BlockTransferCompleted(anEpoch, anEpochStoreEpoch),
        )

        context.extractBecomes() should matchPattern {
          case Seq(
                IssConsensusModule(
                  `DefaultEpochLength`,
                  None, // snapshotAdditionalInfo
                  `aTopologyInfo`,
                  `anEpochInfo`,
                  futurePbftMessageQueue,
                  Seq(), // queuedConsensusMessages
                )
              ) if futurePbftMessageQueue.isEmpty =>
        }
      }
    }
  }

  private def createCatchupBehavior(
      pbftMessageQueue: mutable.Queue[SignedMessage[PbftNetworkMessage]] = mutable.Queue.empty,
      availabilityModuleRef: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]] =
        fakeModuleExpectingSilence,
      outputModuleRef: ModuleRef[Output.Message[ProgrammableUnitTestEnv]] =
        fakeModuleExpectingSilence,
      p2pNetworkOutModuleRef: ModuleRef[P2PNetworkOut.Message] = fakeModuleExpectingSilence,
      epochLength: EpochLength = DefaultEpochLength,
      topologyInfo: OrderingTopologyInfo[ProgrammableUnitTestEnv] = aTopologyInfo,
      epochStore: EpochStore[ProgrammableUnitTestEnv] =
        new InMemoryUnitTestEpochStore[ProgrammableUnitTestEnv],
      preConfiguredInitialEpochState: Option[
        ContextType => EpochState[ProgrammableUnitTestEnv]
      ] = None,
      segmentModuleFactoryFunction: () => ModuleRef[ConsensusSegment.Message] = () =>
        fakeIgnoringModule,
      maybeOnboardingStateTransferManager: Option[StateTransferManager[ProgrammableUnitTestEnv]] =
        None,
      maybeCatchupDetector: Option[CatchupDetector] = None,
  ): (ContextType, CatchupBehavior[ProgrammableUnitTestEnv]) = {
    implicit val context: ContextType = new ProgrammableUnitTestContext

    implicit val metricsContext: MetricsContext = MetricsContext.Empty
    implicit val config: BftBlockOrderer.Config = BftBlockOrderer.Config()

    val dependencies = ConsensusModuleDependencies[ProgrammableUnitTestEnv](
      availabilityModuleRef,
      outputModuleRef,
      p2pNetworkOutModuleRef,
    )

    val latestCompletedEpochFromStore =
      epochStore.latestEpoch(includeInProgress = false)(TraceContext.empty)()

    val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

    val initialEpochState =
      preConfiguredInitialEpochState
        .map(_(context))
        .getOrElse {
          val latestEpochFromStore =
            epochStore.latestEpoch(includeInProgress = true)(TraceContext.empty)()
          val epoch = EpochState.Epoch(
            latestEpochFromStore.info,
            topologyInfo.currentMembership,
            topologyInfo.previousMembership,
            SimpleLeaderSelectionPolicy,
          )
          val segmentModuleRefFactory = createSegmentModuleRefFactory(segmentModuleFactoryFunction)(
            context,
            epoch,
            fakeCryptoProvider,
            latestCompletedEpochFromStore.lastBlockCommits,
            epochStore.loadEpochProgress(latestEpochFromStore.info)(TraceContext.empty)(),
          )
          new EpochState[ProgrammableUnitTestEnv](
            epoch,
            clock,
            abort = fail(_),
            metrics,
            segmentModuleRefFactory,
            loggerFactory = loggerFactory,
            timeouts = timeouts,
          )
        }

    val initialState = CatchupBehavior.InitialState(
      aTopologyInfo,
      initialEpochState,
      latestCompletedEpochFromStore,
      pbftMessageQueue,
      maybeCatchupDetector.getOrElse(new DefaultCatchupDetector(topologyInfo.currentMembership)),
    )
    val moduleRefFactory = createSegmentModuleRefFactory(segmentModuleFactoryFunction)

    context ->
      new CatchupBehavior(
        epochLength,
        initialState,
        epochStore,
        clock,
        metrics,
        moduleRefFactory,
        dependencies,
        loggerFactory,
        timeouts,
      )(maybeOnboardingStateTransferManager)
  }

  def createSegmentModuleRefFactory(
      segmentModuleFactoryFunction: () => ModuleRef[ConsensusSegment.Message]
  ): SegmentModuleRefFactory[ProgrammableUnitTestEnv] =
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
}

object CatchupBehaviorTest {

  private val anEpochInfo: EpochInfo = EpochInfo(
    EpochNumber(1),
    BlockNumber(1),
    EpochLength(20),
    TopologyActivationTime(CantonTimestamp.Epoch),
  )
  private val aMembership = Membership(selfId, otherPeers = Set(fakeSequencerId("other")))
  private val anEpoch = EpochState.Epoch(
    anEpochInfo,
    currentMembership = aMembership,
    previousMembership = aMembership,
    SimpleLeaderSelectionPolicy,
  )
  private val anEpochStoreEpoch = EpochStore.Epoch(anEpochInfo, Seq.empty)

  private val anOrderingTopology = aMembership.orderingTopology
  private val aTopologyInfo = OrderingTopologyInfo[ProgrammableUnitTestEnv](
    selfId,
    anOrderingTopology,
    fakeCryptoProvider,
    previousTopology = anOrderingTopology, // Not relevant
    fakeCryptoProvider,
  )
}
