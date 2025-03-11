// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.EpochInProgress
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisPreviousEpochMaxBftTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferBehavior.StateTransferType
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput.Mode
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

class StateTransferBehaviorTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  import StateTransferBehaviorTest.*
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
        when(stateTransferManagerMock.inBlockTransfer) thenReturn false
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
  }

  // TODO(#24268) test other cases
  "receiving a new epoch topology message from the first epoch after state transfer" should {
    "transition back to consensus mode" in {
      val (context, catchupBehavior) = createCatchupBehavior()
      implicit val ctx: ContextType = context

      catchupBehavior.receive(
        Consensus.NewEpochTopology(
          EpochNumber.First,
          aMembership,
          fakeCryptoProvider,
          GenesisPreviousEpochMaxBftTime,
          Mode.StateTransfer.LastBlock,
        )
      )

      context.extractBecomes() should matchPattern {
        case Seq(
              IssConsensusModule(
                `DefaultEpochLength`,
                None, // snapshotAdditionalInfo
                _, // TODO(#24268) test topology
                futurePbftMessageQueue,
                Seq(), // queuedConsensusMessages
              )
            ) if futurePbftMessageQueue.isEmpty =>
        // TODO(#24268) test resending new epoch topology message
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
  ): (ContextType, StateTransferBehavior[ProgrammableUnitTestEnv]) = {
    implicit val context: ContextType = new ProgrammableUnitTestContext

    implicit val metricsContext: MetricsContext = MetricsContext.Empty
    implicit val config: BftBlockOrdererConfig = BftBlockOrdererConfig()

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

    val initialState = StateTransferBehavior.InitialState(
      latestCompletedEpochFromStore.info.number,
      aTopologyInfo,
      initialEpochState,
      latestCompletedEpochFromStore,
      pbftMessageQueue,
    )
    val moduleRefFactory = createSegmentModuleRefFactory(segmentModuleFactoryFunction)

    context ->
      new StateTransferBehavior(
        epochLength,
        initialState,
        StateTransferType.Catchup,
        maybeCatchupDetector.getOrElse(
          new DefaultCatchupDetector(topologyInfo.currentMembership, loggerFactory)
        ),
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

object StateTransferBehaviorTest {

  private val anEpochInfo: EpochInfo = EpochInfo(
    EpochNumber(1),
    BlockNumber(1),
    EpochLength(20),
    TopologyActivationTime(CantonTimestamp.Epoch),
    CantonTimestamp.MinValue,
  )
  private val aMembership =
    Membership.forTesting(selfId, otherPeers = Set(fakeSequencerId("other")))
  private val anEpoch = EpochState.Epoch(
    anEpochInfo,
    currentMembership = aMembership,
    previousMembership = aMembership,
  )
  private val anEpochStoreEpoch = EpochStore.Epoch(anEpochInfo, Seq.empty)

  private val anOrderingTopology = aMembership.orderingTopology
  private val aTopologyInfo = OrderingTopologyInfo[ProgrammableUnitTestEnv](
    selfId,
    anOrderingTopology,
    fakeCryptoProvider,
    aMembership.leaders,
    previousTopology = anOrderingTopology, // Not relevant
    fakeCryptoProvider,
    aMembership.leaders,
  )
}
