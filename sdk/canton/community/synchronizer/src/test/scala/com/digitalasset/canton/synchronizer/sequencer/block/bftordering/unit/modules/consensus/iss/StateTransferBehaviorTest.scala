// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.EpochInProgress
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferBehavior.StateTransferType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  DelegationCryptoProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.BlockStored
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PbftNetworkMessage,
  PrePrepare,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.IssConsensusModuleTest.myId
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.util.Random

class StateTransferBehaviorTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with HasExecutionContext {

  import IssConsensusModuleTest.*
  import StateTransferBehaviorTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  "StateTransferBehavior" when {

    "ready" should {
      "init" in {
        val (context, stateTransferBehavior) = createStateTransferBehavior()

        stateTransferBehavior.ready(context.self)

        context.extractSelfMessages() shouldBe Seq(Consensus.Init)
      }
    }

    "init" should {
      "cancel the current epoch" in {
        val epochStateMock = mock[EpochState[ProgrammableUnitTestEnv]]
        when(epochStateMock.epoch) thenReturn anEpoch
        val (context, stateTransferBehavior) =
          createStateTransferBehavior(preConfiguredInitialEpochState = Some(_ => epochStateMock))
        implicit val ctx: ContextType = context

        stateTransferBehavior.receive(Consensus.Init)

        verify(epochStateMock, times(1)).notifyEpochCancellationToSegments(EpochNumber(1))

        succeed
      }
    }

    "all segments confirm cancelling the epoch" should {
      "start state transfer" in {
        forAll(List(StateTransferType.Catchup, StateTransferType.Onboarding)) { stateTransferType =>
          val epochStateMock = mock[EpochState[ProgrammableUnitTestEnv]]
          when(epochStateMock.epoch) thenReturn anEpoch
          val stateTransferManagerMock = mock[StateTransferManager[ProgrammableUnitTestEnv]]
          when(stateTransferManagerMock.inStateTransfer) thenReturn false
          val epochStoreMock = mock[EpochStore[ProgrammableUnitTestEnv]]
          when(
            epochStoreMock.latestEpoch(includeInProgress = eqTo(false))(any[TraceContext])
          ) thenReturn (() => anEpochStoreEpoch)
          val (context, stateTransferBehavior) =
            createStateTransferBehavior(
              preConfiguredInitialEpochState = Some(_ => epochStateMock),
              maybeStateTransferManager = Some(stateTransferManagerMock),
              epochStore = epochStoreMock,
              stateTransferType = stateTransferType,
            )
          implicit val ctx: ContextType = context

          stateTransferBehavior.receive(Consensus.SegmentCancelledEpoch)

          verify(stateTransferManagerMock, never).startCatchUp(
            any[Membership],
            any[CryptoProvider[ProgrammableUnitTestEnv]],
            any[EpochStore.Epoch],
            any[EpochNumber],
          )(any[String => Nothing])(any[ContextType], any[TraceContext])

          stateTransferBehavior.receive(Consensus.SegmentCancelledEpoch)

          stateTransferType match {
            case StateTransferType.Onboarding =>
              verify(epochStoreMock, times(1)).startEpoch(anEpoch.info)
            case StateTransferType.Catchup =>
              verify(epochStoreMock, never).startEpoch(any[EpochInfo])(any[TraceContext])
              verify(stateTransferManagerMock, times(1)).startCatchUp(
                eqTo(anEpoch.currentMembership),
                any[CryptoProvider[ProgrammableUnitTestEnv]],
                eqTo(anEpochStoreEpoch),
                eqTo(anEpoch.info.number),
              )(any[String => Nothing])(any[ContextType], any[TraceContext])
          }

          succeed
        }
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
        val (context, stateTransferBehavior) =
          createStateTransferBehavior(
            maybeStateTransferManager = Some(stateTransferManagerMock),
            epochStore = epochStoreMock,
          )
        implicit val ctx: ContextType = context

        val aStateTransferMessage = BlockStored(aCommitCert, myId)
        stateTransferBehavior.receive(aStateTransferMessage)

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
        val (context, stateTransferBehavior) = createStateTransferBehavior()
        implicit val ctx: ContextType = context

        stateTransferBehavior.handleStateTransferMessageResult(
          StateTransferMessageResult.Continue,
          "aMessageType",
        )

        context.runPipedMessages() shouldBe empty
        context.extractSelfMessages() shouldBe empty
        context.extractBecomes() shouldBe empty
        context.delayedMessages shouldBe empty
      }
    }

    "handling a 'NothingToTransfer' result of processing a state transfer message" should {
      "retry state transfer if a new epoch topology message has not been received, " +
        "and the end epoch has not yet been transferred" in {
          Table(
            ("new epoch topology message", "minimum state transfer end epoch"),
            (None, None),
            (
              Some(
                Consensus.NewEpochTopology(
                  EpochNumber.First,
                  aMembership,
                  aFakeCryptoProviderInstance,
                )
              ),
              Some(EpochNumber(7)),
            ),
          ).forEvery { (newEpochTopologyMessage, minimumEndEpoch) =>
            val stateTransferManagerMock = mock[StateTransferManager[ProgrammableUnitTestEnv]]
            val epochStoreMock = mock[EpochStore[ProgrammableUnitTestEnv]]
            when(
              epochStoreMock.latestEpoch(any[Boolean])(any[TraceContext])
            ) thenReturn (() => anEpochStoreEpoch)
            when(
              epochStoreMock.loadEpochProgress(eqTo(anEpochStoreEpoch.info))(any[TraceContext])
            ) thenReturn (() => EpochInProgress())
            val (context, stateTransferBehavior) =
              createStateTransferBehavior(
                epochStore = epochStoreMock,
                maybeStateTransferManager = Some(stateTransferManagerMock),
                minimumStateTransferEndEpoch = minimumEndEpoch,
              )
            implicit val ctx: ContextType = context

            stateTransferBehavior.maybeLastReceivedEpochTopology = newEpochTopologyMessage

            val epochNumber = anEpochStoreEpoch.info.number
            stateTransferBehavior.handleStateTransferMessageResult(
              StateTransferMessageResult.NothingToStateTransfer(otherIds.head),
              "aMessageType",
            )

            verify(stateTransferManagerMock, times(1)).stateTransferNewEpoch(
              eqTo(epochNumber),
              eqTo(aMembership),
              eqTo(aFakeCryptoProviderInstance),
            )(any[String => Nothing])(eqTo(ctx), any[TraceContext])
            succeed
          }
        }
    }

    "transition back to consensus mode if a new epoch topology message is set, " +
      "and transferred at least up to the minimum end epoch if it's defined" in {
        Table(
          "minimum state transfer end epoch",
          None,
          Some(EpochNumber(anEpochInfo.number)), // the same as the "current epoch", see below
        ).forEvery { minimumEndEpoch =>
          val epochStoreMock = mock[EpochStore[ProgrammableUnitTestEnv]]
          when(
            epochStoreMock.latestEpoch(any[Boolean])(any[TraceContext])
          ) thenReturn (() => anEpochStoreEpoch)
          when(
            epochStoreMock.loadEpochProgress(eqTo(anEpochInfo))(any[TraceContext])
          ) thenReturn (() => EpochInProgress())
          val (context, stateTransferBehavior) =
            createStateTransferBehavior(
              epochStore = epochStoreMock,
              minimumStateTransferEndEpoch = minimumEndEpoch,
            )
          implicit val ctx: ContextType = context

          stateTransferBehavior.maybeLastReceivedEpochTopology = Some(
            Consensus.NewEpochTopology(
              EpochNumber.First,
              aMembership,
              aFakeCryptoProviderInstance,
            )
          )

          stateTransferBehavior.handleStateTransferMessageResult(
            StateTransferMessageResult.NothingToStateTransfer(otherIds.head),
            "aMessageType",
          )

          val becomes = context.extractBecomes()
          inside(becomes) {
            case Seq(
                  consensusModule @ IssConsensusModule(
                    TestEpochLength,
                    None, // snapshotAdditionalInfo
                    `aTopologyInfo`,
                    futurePbftMessageQueue,
                    Seq(), // queuedConsensusMessages
                  )
                ) if futurePbftMessageQueue.isEmpty =>
              // A successful check below means that the new epoch topology message has been resent
              //  and processed by the Consensus module.
              consensusModule.isInitComplete shouldBe true
          }
        }
      }
  }

  "receiving a new epoch topology message" should {
    "store the new epoch and update availability topology" in {
      val epochStoreMock = mock[EpochStore[ProgrammableUnitTestEnv]]
      when(
        epochStoreMock.latestEpoch(any[Boolean])(any[TraceContext])
      ) thenReturn (() => anEpochStoreEpoch)
      when(
        epochStoreMock.loadEpochProgress(eqTo(anEpochStoreEpoch.info))(any[TraceContext])
      ) thenReturn (() => EpochInProgress())
      val stateTransferManagerMock = mock[StateTransferManager[ProgrammableUnitTestEnv]]
      val availabilityMock = mock[ModuleRef[Availability.Message[ProgrammableUnitTestEnv]]]
      val (context, stateTransferBehavior) =
        createStateTransferBehavior(
          epochStore = epochStoreMock,
          maybeStateTransferManager = Some(stateTransferManagerMock),
          availabilityModuleRef = availabilityMock,
        )
      implicit val ctx: ContextType = context

      val startEpochNumber = anEpochInfo.number
      val newEpochNumber = EpochNumber(startEpochNumber + 1)
      val newEpoch = EpochInfo(
        newEpochNumber,
        BlockNumber(11L),
        TestEpochLength,
        TopologyActivationTime(CantonTimestamp.MinValue),
      )

      stateTransferBehavior.receive(
        Consensus.NewEpochTopology(
          newEpochNumber,
          aMembership,
          aFakeCryptoProviderInstance,
        )
      )

      verify(stateTransferManagerMock, times(1)).cancelTimeoutForEpoch(eqTo(startEpochNumber))(
        any[TraceContext]
      )
      verify(epochStoreMock, times(1)).completeEpoch(startEpochNumber)
      verify(epochStoreMock, times(1)).startEpoch(newEpoch)
      verify(availabilityMock, times(1)).asyncSend(
        Availability.Consensus.UpdateTopologyDuringStateTransfer[ProgrammableUnitTestEnv](
          aMembership.orderingTopology,
          DelegationCryptoProvider(aFakeCryptoProviderInstance, aFakeCryptoProviderInstance),
        )
      )

      succeed
    }

    "receiving a new epoch stored message" should {
      "set the epoch state and start state-transferring the epoch" in {
        val stateTransferManagerMock = mock[StateTransferManager[ProgrammableUnitTestEnv]]
        val (context, stateTransferBehavior) =
          createStateTransferBehavior(maybeStateTransferManager = Some(stateTransferManagerMock))
        implicit val ctx: ContextType = context

        stateTransferBehavior.receive(
          Consensus.NewEpochStored(
            anEpochInfo,
            aMembership,
            aFakeCryptoProviderInstance,
          )
        )

        // Should have set the new epoch state.
        stateTransferBehavior should matchPattern {
          case StateTransferBehavior(
                TestEpochLength,
                _,
                _,
                _,
                `anEpochInfo`,
                _,
              ) =>
        }

        verify(stateTransferManagerMock, times(1)).stateTransferNewEpoch(
          eqTo(anEpochInfo.number),
          eqTo(aMembership),
          eqTo(aFakeCryptoProviderInstance),
        )(any[String => Nothing])(eqTo(ctx), any[TraceContext])

        succeed
      }
    }
  }

  private def createStateTransferBehavior(
      pbftMessageQueue: mutable.Queue[SignedMessage[PbftNetworkMessage]] = mutable.Queue.empty,
      availabilityModuleRef: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]] =
        fakeModuleExpectingSilence,
      outputModuleRef: ModuleRef[Output.Message[ProgrammableUnitTestEnv]] =
        fakeModuleExpectingSilence,
      p2pNetworkOutModuleRef: ModuleRef[P2PNetworkOut.Message] = fakeModuleExpectingSilence,
      epochLength: EpochLength = TestEpochLength,
      topologyInfo: OrderingTopologyInfo[ProgrammableUnitTestEnv] = aTopologyInfo,
      epochStore: EpochStore[ProgrammableUnitTestEnv] =
        new InMemoryUnitTestEpochStore[ProgrammableUnitTestEnv],
      preConfiguredInitialEpochState: Option[
        ContextType => EpochState[ProgrammableUnitTestEnv]
      ] = None,
      segmentModuleFactoryFunction: () => ModuleRef[ConsensusSegment.Message] = () =>
        fakeIgnoringModule,
      maybeStateTransferManager: Option[StateTransferManager[ProgrammableUnitTestEnv]] = None,
      maybeCatchupDetector: Option[CatchupDetector] = None,
      minimumStateTransferEndEpoch: Option[EpochNumber] = None,
      stateTransferType: StateTransferType = StateTransferType.Catchup,
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
            failingCryptoProvider,
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
      stateTransferStartEpoch = latestCompletedEpochFromStore.info.number,
      minimumStateTransferEndEpoch,
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
        stateTransferType,
        maybeCatchupDetector.getOrElse(
          new DefaultCatchupDetector(topologyInfo.currentMembership, loggerFactory)
        ),
        epochStore,
        clock,
        metrics,
        moduleRefFactory,
        new Random(4),
        dependencies,
        loggerFactory,
        timeouts,
      )(maybeStateTransferManager)
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

  private val TestEpochLength = EpochLength(10L)

  private val anEpochInfo: EpochInfo = EpochInfo(
    EpochNumber(1),
    BlockNumber(1),
    TestEpochLength,
    TopologyActivationTime(CantonTimestamp.Epoch),
  )
  private val aMembership =
    Membership.forTesting(myId, otherNodes = Set(BftNodeId("other")))
  private val anEpoch = EpochState.Epoch(
    anEpochInfo,
    currentMembership = aMembership,
    previousMembership = aMembership,
  )
  private val anEpochStoreEpoch = EpochStore.Epoch(anEpochInfo, Seq.empty)

  private val anOrderingTopology = aMembership.orderingTopology
  private val aFakeCryptoProviderInstance: CryptoProvider[ProgrammableUnitTestEnv] =
    failingCryptoProvider
  private val aTopologyInfo = OrderingTopologyInfo[ProgrammableUnitTestEnv](
    myId,
    anOrderingTopology,
    aFakeCryptoProviderInstance,
    aMembership.leaders,
    previousTopology = anOrderingTopology, // Not relevant
    aFakeCryptoProviderInstance,
    aMembership.leaders,
  )

  private def aCommitCert(implicit synchronizerProtocolVersion: ProtocolVersion) =
    CommitCertificate(
      PrePrepare
        .create(
          blockMetadata = BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
          viewNumber = ViewNumber.First,
          block = OrderingBlock(Seq.empty),
          canonicalCommitSet = CanonicalCommitSet.empty,
          from = myId,
        )
        .fakeSign,
      commits = Seq.empty,
    )
}
