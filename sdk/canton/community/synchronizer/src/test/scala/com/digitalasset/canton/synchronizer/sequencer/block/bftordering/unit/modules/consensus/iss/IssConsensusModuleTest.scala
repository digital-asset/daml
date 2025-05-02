// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.{
  BftBlockOrdererConfig,
  FingerprintKeyId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.{
  Block,
  EpochInProgress,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.{
  GenesisEpoch,
  GenesisEpochInfo,
  GenesisEpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.GenericInMemoryEpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  Genesis,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions.RetransmissionsManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.{
  CatchupDetector,
  DefaultCatchupDetector,
  StateTransferBehavior,
  StateTransferManager,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory.GenericInMemoryOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftKeyId,
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.{
  NodeActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  MessageAuthorizer,
  OrderingTopology,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.ConsensusMessage.{
  CompleteEpochStored,
  PbftUnverifiedNetworkMessage,
  PbftVerifiedNetworkMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.{
  NewEpochTopology,
  ProtocolMessage,
  RetransmissionsMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PbftNetworkMessage,
  PrePrepare,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus.EpochStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString
import org.mockito.Mockito
import org.scalatest.TryValues
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level
import org.slf4j.event.Level.ERROR

import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, Try}

class IssConsensusModuleTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with HasExecutionContext {

  import IssConsensusModuleTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty
  private implicit val config: BftBlockOrdererConfig = BftBlockOrdererConfig()

  private val blockOrder4Nodes =
    Iterator.continually(allIds).flatten.take(DefaultEpochLength.toInt).toSeq
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

      "do nothing if the previous epoch hasn't been completed" in {
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
                latestCompletedEpochFromStore,
                _,
              )
            ),
          )
        implicit val ctx: ContextType = context

        consensus.receive(Consensus.Start)
        consensus.receive(
          Consensus.NewEpochTopology(
            EpochNumber(2L),
            aMembership,
            failingCryptoProvider,
          )
        )

        verify(epochStore, never).startEpoch(any[EpochInfo])(any[TraceContext])
        succeed
      }

      "start a new epoch when it hasn't been started only if the node is part of the topology" in {
        Table(
          ("topology nodes", "startEpoch calls count"),
          (allIds, times(1)),
          (otherIds, never),
        ).forEvery { case (nodes, expectedStartEpochCalls) =>
          val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
          val latestTopologyActivationTime = TopologyActivationTime(aTimestamp)
          val latestCompletedEpochFromStore = EpochStore.Epoch(
            EpochInfo(
              EpochNumber.First,
              BlockNumber.First,
              epochLength,
              latestTopologyActivationTime,
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

          // emulate time advancing for the next epoch's ordering topology activation
          val nextTopologyActivationTime =
            TopologyActivationTime(latestTopologyActivationTime.value.immediateSuccessor)

          consensus.receive(Consensus.Start)
          consensus.receive(
            Consensus.NewEpochTopology(
              EpochNumber(1L),
              Membership(
                myId,
                OrderingTopology.forTesting(
                  nodes = nodes.toSet,
                  activationTime = nextTopologyActivationTime,
                ),
                allIds,
              ),
              failingCryptoProvider,
            )
          )

          verify(epochStore, expectedStartEpochCalls).startEpoch(
            latestCompletedEpochFromStore.info
              .next(epochLength, nextTopologyActivationTime)
          )
          succeed
        }
      }

      "start segment modules only once after restart with delayed Start signal" in {
        val segmentModuleMock = mock[ModuleRef[ConsensusSegment.Message]]
        val stateTransferManagerMock = mock[StateTransferManager[ProgrammableUnitTestEnv]]
        when(stateTransferManagerMock.inStateTransfer).thenReturn(true)
        val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
        val cryptoProvider = failingCryptoProvider[ProgrammableUnitTestEnv]

        val aTopologyActivationTime = Genesis.GenesisTopologyActivationTime
        val aStartEpoch =
          GenesisEpoch.info.next(epochLength, aTopologyActivationTime)
        val newEpochInfo =
          aStartEpoch.next(epochLength, aTopologyActivationTime)
        val membership =
          Membership(
            myId,
            anOrderingTopology.copy(activationTime = aTopologyActivationTime),
            allIds,
          )
        val latestCompletedEpochFromStore = EpochStore.Epoch(
          EpochInfo(
            aStartEpoch.number,
            aStartEpoch.startBlockNumber,
            aStartEpoch.length,
            aTopologyActivationTime,
          ),
          Seq.empty,
        )
        val completedBlocks: Seq[Block] =
          (aStartEpoch.startBlockNumber until aStartEpoch.startBlockNumber + epochLength).map {
            idx =>
              val blockNum = BlockNumber(idx)
              val pp = PrePrepare
                .create(
                  BlockMetadata(aStartEpoch.number, blockNum),
                  ViewNumber.First,
                  OrderingBlock.empty,
                  CanonicalCommitSet.empty,
                  myId,
                )
                .fakeSign
              Block(
                aStartEpoch.number,
                blockNum,
                CommitCertificate(pp, Seq.empty),
              )
          }

        when(epochStore.latestEpoch(includeInProgress = false)).thenReturn(() =>
          latestCompletedEpochFromStore
        )
        when(epochStore.latestEpoch(includeInProgress = true)).thenReturn(() =>
          latestCompletedEpochFromStore
        )
        when(epochStore.loadEpochProgress(latestCompletedEpochFromStore.info)).thenReturn(() =>
          EpochInProgress(
            completedBlocks = completedBlocks,
            pbftMessagesForIncompleteBlocks = Seq.empty,
          )
        )
        when(epochStore.completeEpoch(aStartEpoch.number)).thenReturn(() => ())
        when(epochStore.startEpoch(any[EpochInfo])(any[TraceContext])).thenReturn(() => ())

        val (context, consensus) =
          createIssConsensusModule(
            p2pNetworkOutModuleRef = fakeIgnoringModule,
            segmentModuleFactoryFunction = _ => segmentModuleMock,
            epochStore = epochStore,
            completedBlocks = completedBlocks,
            maybeOnboardingStateTransferManager = Some(stateTransferManagerMock),
            resolveAwaits = true,
          )
        implicit val ctx: ContextType = context

        consensus.receive(
          Consensus.NewEpochTopology(
            newEpochInfo.number,
            membership,
            cryptoProvider,
          )
        )
        context.runPipedMessages() shouldBe empty

        consensus.receive(Consensus.Start)
        context.runPipedMessagesThenVerifyAndReceiveOnModule(consensus) { msg =>
          msg shouldBe Consensus.NewEpochStored(
            newEpochInfo,
            membership,
            cryptoProvider,
          )
        }

        val order = Mockito.inOrder(stateTransferManagerMock, segmentModuleMock)
        order
          .verify(segmentModuleMock, times(membership.orderingTopology.nodes.size))
          .asyncSend(ConsensusSegment.Start)
        succeed
      }

      "do nothing if a new epoch is already in progress" in {
        val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
        val aTopologyActivationTime = TopologyActivationTime(aTimestamp)
        val latestCompletedEpochFromStore = EpochStore.Epoch(
          EpochInfo(
            EpochNumber.First,
            BlockNumber.First,
            epochLength,
            aTopologyActivationTime,
          ),
          Seq.empty,
        )
        when(epochStore.latestEpoch(includeInProgress = false)).thenReturn(() =>
          latestCompletedEpochFromStore
        )
        when(epochStore.latestEpoch(includeInProgress = true)).thenReturn(() =>
          EpochStore.Epoch(
            latestCompletedEpochFromStore.info
              .next(epochLength, aTopologyActivationTime),
            Seq.empty,
          )
        )
        val activeStartingEpochInfo =
          latestCompletedEpochFromStore.info.next(
            epochLength,
            aTopologyActivationTime,
          )
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
            aMembership,
            failingCryptoProvider,
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
                aMembership,
                failingCryptoProvider,
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

    "completing an epoch" should {

      "start a new epoch if its topology has already been received but only if the node is part of the topology" in {
        Table(
          ("topology nodes", "startEpoch calls count"),
          (allIds, times(1)),
          (otherIds, never),
        ).forEvery { case (nodes, expectedStartEpochCalls) =>
          val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
          val latestTopologyActivationTime = TopologyActivationTime(aTimestamp)
          val latestCompletedEpochFromStore = EpochStore.Epoch(
            EpochInfo(
              EpochNumber.First,
              BlockNumber.First,
              epochLength,
              latestTopologyActivationTime,
            ),
            Seq.empty,
          )

          when(epochStore.latestEpoch(anyBoolean)(any[TraceContext])).thenReturn(() =>
            latestCompletedEpochFromStore
          )
          when(epochStore.startEpoch(latestCompletedEpochFromStore.info)).thenReturn(() => ())

          // emulate time advancing for the next epoch's ordering topology activation
          val nextTopologyActivationTime =
            TopologyActivationTime(latestTopologyActivationTime.value.immediateSuccessor)

          val (context, consensus) =
            createIssConsensusModule(
              epochStore = epochStore,
              preConfiguredInitialEpochState =
                Some(newEpochState(latestCompletedEpochFromStore, _)),
              newEpochTopology = Some(
                NewEpochTopology(
                  EpochNumber(1L),
                  Membership(
                    myId,
                    OrderingTopology.forTesting(
                      nodes = nodes.toSet,
                      activationTime = nextTopologyActivationTime,
                    ),
                    nodes,
                  ),
                  failingCryptoProvider,
                )
              ),
            )
          implicit val ctx: ContextType = context

          consensus.receive(Consensus.Start)
          consensus.receive(CompleteEpochStored(latestCompletedEpochFromStore, Seq.empty))

          verify(epochStore, expectedStartEpochCalls).startEpoch(
            latestCompletedEpochFromStore.info
              .next(epochLength, nextTopologyActivationTime)
          )
          succeed
        }
      }
    }

    "completing state transfer" should {
      "complete init, dequeue all messages, and process the new epoch topology message" in {
        val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
        val latestTopologyActivationTime = TopologyActivationTime(aTimestamp)
        val latestCompletedEpochFromStore = EpochStore.Epoch(
          EpochInfo(
            EpochNumber.First,
            BlockNumber.First,
            epochLength,
            latestTopologyActivationTime,
          ),
          Seq.empty,
        )

        when(epochStore.latestEpoch(anyBoolean)(any[TraceContext])).thenReturn(() =>
          latestCompletedEpochFromStore
        )
        when(epochStore.startEpoch(latestCompletedEpochFromStore.info)).thenReturn(() => ())

        val futurePbftMessageQueue: mutable.Queue[SignedMessage[PbftNetworkMessage]] =
          new mutable.Queue()
        val aDummyMessage =
          ConsensusSegment.ConsensusMessage.ViewChange
            .create(
              BlockMetadata(EpochNumber.First, BlockNumber.First),
              segmentIndex = 1,
              viewNumber = ViewNumber.First,
              consensusCerts = Seq.empty,
              from = myId,
            )
            .fakeSign
        futurePbftMessageQueue.enqueue(aDummyMessage)
        val postponedConsensusMessageQueue =
          new mutable.Queue[Consensus.Message[ProgrammableUnitTestEnv]]()
        postponedConsensusMessageQueue.enqueue(PbftVerifiedNetworkMessage(aDummyMessage))

        val (context, consensus) =
          createIssConsensusModule(
            epochStore = epochStore,
            preConfiguredInitialEpochState = Some(newEpochState(latestCompletedEpochFromStore, _)),
            futurePbftMessageQueue = futurePbftMessageQueue,
            postponedConsensusMessageQueue = postponedConsensusMessageQueue,
          )
        implicit val ctx: ContextType = context

        // emulate time advancing for the next epoch's ordering topology activation
        val nextTopologyActivationTime =
          TopologyActivationTime(latestTopologyActivationTime.value.immediateSuccessor)

        consensus.receive(
          Consensus.StateTransferCompleted(
            Consensus.NewEpochTopology(
              EpochNumber(1L),
              Membership(
                myId,
                OrderingTopology.forTesting(
                  nodes = allIds.toSet,
                  activationTime = nextTopologyActivationTime,
                ),
                allIds,
              ),
              failingCryptoProvider,
            )
          )
        )

        consensus.isInitComplete shouldBe true
        futurePbftMessageQueue shouldBe empty
        postponedConsensusMessageQueue shouldBe empty
        context.extractSelfMessages() should contain only PbftVerifiedNetworkMessage(aDummyMessage)
        verify(epochStore, times(1)).startEpoch(
          latestCompletedEpochFromStore.info.next(epochLength, nextTopologyActivationTime)
        )
        succeed
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
            segmentModuleFactoryFunction = _ => fakeModuleExpectingSilence,
          )
        implicit val ctx: ContextType = context

        consensus.getEpochState.epochCompletionStatus.isComplete shouldBe true
        consensus.receive(Consensus.Start)

        context.extractSelfMessages() should matchPattern {
          case Seq(Consensus.NewEpochTopology(epochNumber, membership, _))
              if epochNumber == EpochNumber.First && membership.orderingTopology == anOrderingTopology =>
        }
      }

      "advance epoch and wait for the next epoch's topology" when {
        "the current epoch is complete" in {
          val commits =
            Seq(
              Commit
                .create(
                  BlockMetadata(EpochNumber.First, BlockNumber(0L)),
                  ViewNumber.First,
                  Hash.digest(
                    HashPurpose.BftOrderingPbftBlock,
                    ByteString.EMPTY,
                    HashAlgorithm.Sha256,
                  ),
                  CantonTimestamp.Epoch,
                  from = myId,
                )
                .fakeSign
            )
          val epoch0 =
            EpochStore.Epoch(
              EpochInfo(
                EpochNumber.First,
                BlockNumber.First,
                EpochLength(0),
                TopologyActivationTime(aTimestamp),
              ),
              lastBlockCommits = Seq.empty,
            )
          val epoch1 =
            epoch0.copy(
              info = epoch0.info
                .copy(
                  number = EpochNumber(1L),
                  length = EpochLength(1),
                  topologyActivationTime = TopologyActivationTime(
                    aTimestamp.plusSeconds(1)
                  ), // Not relevant for the test but just to avoid confusion
                ),
              lastBlockCommits = commits,
            )

          Table("latest completed epoch from store", epoch0, epoch1).forEvery {
            latestCompletedEpochFromStore =>
              val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
              val prePrepare =
                PrePrepare
                  .create(
                    BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
                    ViewNumber.First,
                    OrderingBlock.empty,
                    CanonicalCommitSet.empty,
                    from = myId,
                  )
                  .fakeSign
              val completedBlocks =
                Seq(
                  EpochStore.Block(
                    EpochNumber(1),
                    BlockNumber(0),
                    CommitCertificate(prePrepare, commits),
                  )
                )
              when(epochStore.latestEpoch(includeInProgress = eqTo(false))(any[TraceContext]))
                .thenReturn(() => latestCompletedEpochFromStore)
              when(epochStore.latestEpoch(includeInProgress = eqTo(true))(any[TraceContext]))
                .thenReturn(() => epoch1)
              when(epochStore.loadEpochProgress(epoch1.info)).thenReturn(() =>
                EpochStore.EpochInProgress(
                  completedBlocks,
                  pbftMessagesForIncompleteBlocks = Seq.empty,
                )
              )
              val (context, consensus) =
                createIssConsensusModule(
                  epochStore = epochStore,
                  segmentModuleFactoryFunction = _ => fakeModuleExpectingSilence,
                  completedBlocks = completedBlocks,
                  resolveAwaits = true,
                )
              implicit val ctx: ContextType = context

              consensus.getEpochState.epochCompletionStatus.isComplete shouldBe true

              when(epochStore.completeEpoch(epoch1.info.number)).thenReturn(() => ())
              consensus.receive(Consensus.Start)

              // Regardless if the epoch completion was stored before the consensus module started, it must be now.
              verify(epochStore, times(1)).completeEpoch(epoch1.info.number)
              consensus.getLatestCompletedEpoch shouldBe epoch1

              consensus.getEpochState.isClosing shouldBe true
              consensus.getEpochState.epoch.info shouldBe epoch1.info
              context.runPipedMessages() shouldBe empty
              context.extractSelfMessages() shouldBe empty
          }
        }
      }

      "complete the epoch when all blocks from all segments complete" in {
        val outputBuffer =
          new ArrayBuffer[Output.Message[ProgrammableUnitTestEnv]](defaultBufferSize)
        val epochLength = DefaultEpochLength

        val (context, consensus) = createIssConsensusModule(
          outputModuleRef = fakeRecordingModule(outputBuffer),
          epochLength = epochLength,
        )
        implicit val ctx: ContextType = context

        consensus.receive(Consensus.Start)

        // Consensus is starting from genesis, so it'll start a new epoch
        val newEpochTopologyMsg = NewEpochTopology(
          EpochNumber.First,
          aMembership,
          failingCryptoProvider,
        )
        val selfSentMessages = context.extractSelfMessages()
        selfSentMessages should matchPattern {
          case Seq(Consensus.NewEpochTopology(epochNumber, membership, _))
              if epochNumber == newEpochTopologyMsg.epochNumber &&
                membership.orderingTopology == newEpochTopologyMsg.membership.orderingTopology =>
        }
        selfSentMessages.foreach(consensus.receive)
        // Store the new epoch and update the epoch state
        context.runPipedMessagesAndReceiveOnModule(consensus)

        // One by one, complete blocks to finish all segments in the epoch
        (0 until epochLength.toInt).foreach { n =>
          val leaderOfBlock = blockOrder4Nodes(n)
          val isLastBlockInEpoch = n == epochLength - 1
          val prePrepare = PrePrepare.create(
            blockMetadata4Nodes(n),
            ViewNumber.First,
            OrderingBlock(oneRequestOrderingBlock.proofs),
            CanonicalCommitSet(Set.empty),
            leaderOfBlock,
          )
          val expectedOrderedBlock = orderedBlockFromPrePrepare(
            prePrepare
          )

          consensus.receive(
            Consensus.ConsensusMessage
              .BlockOrdered(expectedOrderedBlock, CommitCertificate(prePrepare.fakeSign, Seq.empty))
          )
          outputBuffer should contain theSameElementsInOrderAs Seq[
            Output.Message[FakePipeToSelfCellUnitTestEnv]
          ](
            Output.BlockOrdered(
              OrderedBlockForOutput(
                expectedOrderedBlock,
                prePrepare.viewNumber,
                leaderOfBlock,
                isLastBlockInEpoch,
                OrderedBlockForOutput.Mode.FromConsensus,
              )
            )
          )
          outputBuffer.clear()

          if (isLastBlockInEpoch) {
            context.runPipedMessages() should matchPattern {
              case Seq(Consensus.ConsensusMessage.CompleteEpochStored(_, _)) =>
            }
          } else {
            context.extractSelfMessages() shouldBe empty
          }
        }

        succeed
      }

      "start onboarding state transfer when a snapshot is provided" in {
        val segmentModuleMock = mock[ModuleRef[ConsensusSegment.Message]]

        val aStartEpoch = GenesisEpoch.info.next(
          epochLength,
          Genesis.GenesisTopologyActivationTime,
        )
        val aStartEpochNumber = aStartEpoch.number

        val (context, consensus) =
          createIssConsensusModule(
            p2pNetworkOutModuleRef = fakeIgnoringModule,
            // Trigger state transfer for onboarding
            sequencerSnapshotAdditionalInfo = Some(
              SequencerSnapshotAdditionalInfo(
                Map(
                  myId -> NodeActiveAt(
                    timestamp = TopologyActivationTime(CantonTimestamp.Epoch),
                    startEpochNumber = Some(aStartEpochNumber),
                    firstBlockNumberInStartEpoch = Some(aStartEpoch.startBlockNumber),
                    startEpochTopologyQueryTimestamp = Some(aStartEpoch.topologyActivationTime),
                    startEpochCouldAlterOrderingTopology = None,
                    previousBftTime = None,
                    previousEpochTopologyQueryTimestamp = None,
                  )
                )
              )
            ),
            segmentModuleFactoryFunction = _ => segmentModuleMock,
          )
        implicit val ctx: ContextType = context

        consensus.receive(Consensus.Start)

        val becomes = context.extractBecomes()
        println(aStartEpoch)
        becomes should matchPattern {
          case Seq(
                StateTransferBehavior(
                  DefaultEpochLength,
                  `aStartEpochNumber`,
                  None, // minimum state transfer end epoch
                  `aTopologyInfo`,
                  `aStartEpoch`,
                  GenesisEpoch,
                )
              ) =>
        }
        // Should not yet start segment modules
        verify(segmentModuleMock, never).asyncSend(ConsensusSegment.Start)
        succeed
      }

      "start catch-up if the detector says so" in {
        Table[ProtocolMessage](
          "message",
          PbftVerifiedNetworkMessage(
            SignedMessage(
              PrePrepare.create( // Just to trigger the catch-up check
                blockMetadata4Nodes(1),
                ViewNumber.First,
                OrderingBlock(oneRequestOrderingBlock.proofs),
                CanonicalCommitSet(Set.empty),
                allIds(1),
              ),
              Signature.noSignature,
            )
          ),
          RetransmissionsMessage.VerifiedNetworkMessage(
            RetransmissionsMessage.RetransmissionRequest.create(
              EpochStatus(allIds(1), EpochNumber.First, Seq.empty)
            )
          ),
        ).forEvery { message =>
          val stateTransferManagerMock = mock[StateTransferManager[ProgrammableUnitTestEnv]]
          val retransmissionsManagerMock = mock[RetransmissionsManager[ProgrammableUnitTestEnv]]
          val segmentModuleMock = mock[ModuleRef[ConsensusSegment.Message]]
          val catchupDetectorMock = mock[CatchupDetector]
          when(catchupDetectorMock.updateLatestKnownNodeEpoch(any[BftNodeId], any[EpochNumber]))
            .thenReturn(true)
          val catchUpToEpochNumber = Some(EpochNumber(7))
          when(catchupDetectorMock.shouldCatchUpTo(any[EpochNumber])(any[TraceContext]))
            .thenReturn(catchUpToEpochNumber)

          val (context, consensus) =
            createIssConsensusModule(
              p2pNetworkOutModuleRef = fakeIgnoringModule,
              segmentModuleFactoryFunction = _ => segmentModuleMock,
              maybeOnboardingStateTransferManager = Some(stateTransferManagerMock),
              maybeCatchupDetector = Some(catchupDetectorMock),
              maybeRetransmissionsManager = Some(retransmissionsManagerMock),
            )
          implicit val ctx: ContextType = context

          consensus.receive(Consensus.Start)
          consensus.receive(message)

          verify(catchupDetectorMock, times(1))
            .updateLatestKnownNodeEpoch(allIds(1), EpochNumber.First)
          verify(catchupDetectorMock, times(1))
            .shouldCatchUpTo(eqTo(GenesisEpochNumber))(any[TraceContext])
          verify(retransmissionsManagerMock, never)
            .handleMessage(
              any[CryptoProvider[ProgrammableUnitTestEnv]],
              any[RetransmissionsMessage],
            )(any[ContextType], any[TraceContext])
          context.extractBecomes() should matchPattern {
            case Seq(
                  StateTransferBehavior(
                    `DefaultEpochLength`,
                    `GenesisEpochNumber`,
                    `catchUpToEpochNumber`,
                    `aTopologyInfo`,
                    GenesisEpochInfo,
                    EpochStore.Epoch(GenesisEpochInfo, Seq()),
                  )
                ) =>
          }
        }
      }

      "drop remote PBFT messages" when {
        "unauthorized" in {
          val mockMessageAuthorizer = mock[MessageAuthorizer]
          when(mockMessageAuthorizer.isAuthorized(any[BftNodeId], any[BftKeyId])) thenReturn false
          val (context, consensus) =
            createIssConsensusModule(customMessageAuthorizer = Some(mockMessageAuthorizer))
          implicit val ctx: ContextType = context

          val underlyingMessage = mock[ConsensusSegment.ConsensusMessage.PbftNetworkMessage]
          when(underlyingMessage.blockMetadata).thenReturn(
            BlockMetadata(
              EpochNumber.First,
              BlockNumber.First,
            )
          )
          val unauthorizedNodeId = BftNodeId("unauthorized")
          when(underlyingMessage.from) thenReturn unauthorizedNodeId
          when(underlyingMessage.viewNumber).thenReturn(ViewNumber.First)
          val signedMessage = underlyingMessage.fakeSign

          consensus.receive(Consensus.Start)

          assertLogs(
            consensus.receive(PbftUnverifiedNetworkMessage(signedMessage)),
            (logEntry: LogEntry) => {
              logEntry.level shouldBe Level.WARN
              logEntry.message should include(
                "it is unauthorized in the current ordering topology"
              )
            },
          )

          verify(mockMessageAuthorizer).isAuthorized(
            unauthorizedNodeId,
            FingerprintKeyId.toBftKeyId(signedMessage.signature.signedBy),
          )

          context.runPipedMessages() shouldBe empty
        }
      }
    }
  }

  private def newEpochState(
      latestCompletedEpochFromStore: EpochStore.Epoch,
      context: ContextType,
      segmentModuleFactoryFunction: EpochState.Epoch => ModuleRef[ConsensusSegment.Message] = _ =>
        fakeIgnoringModule,
  ): EpochState[ProgrammableUnitTestEnv] = {
    val membership = Membership.forTesting(myId)
    val epochStateEpoch =
      EpochState.Epoch(
        latestCompletedEpochFromStore.info,
        currentMembership = membership,
        previousMembership = membership,
      )
    new EpochState[ProgrammableUnitTestEnv](
      epoch = epochStateEpoch,
      clock,
      fail(_),
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      createSegmentModuleRefFactory(segmentModuleFactoryFunction)(
        context,
        epochStateEpoch,
        failingCryptoProvider,
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
      topologyInfo: OrderingTopologyInfo[ProgrammableUnitTestEnv] = aTopologyInfo,
      epochStore: EpochStore[ProgrammableUnitTestEnv] =
        new InMemoryUnitTestEpochStore[ProgrammableUnitTestEnv],
      preConfiguredInitialEpochState: Option[
        ContextType => EpochState[ProgrammableUnitTestEnv]
      ] = None,
      sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo] = None,
      segmentModuleFactoryFunction: EpochState.Epoch => ModuleRef[ConsensusSegment.Message] = _ =>
        fakeIgnoringModule,
      maybeOnboardingStateTransferManager: Option[StateTransferManager[ProgrammableUnitTestEnv]] =
        None,
      maybeCatchupDetector: Option[CatchupDetector] = None,
      maybeRetransmissionsManager: Option[RetransmissionsManager[ProgrammableUnitTestEnv]] = None,
      newEpochTopology: Option[NewEpochTopology[ProgrammableUnitTestEnv]] = None,
      completedBlocks: Seq[EpochStore.Block] = Seq.empty,
      resolveAwaits: Boolean = false,
      customMessageAuthorizer: Option[MessageAuthorizer] = None,
      futurePbftMessageQueue: mutable.Queue[SignedMessage[PbftNetworkMessage]] =
        new mutable.Queue(),
      postponedConsensusMessageQueue: mutable.Queue[Consensus.Message[ProgrammableUnitTestEnv]] =
        new mutable.Queue[Consensus.Message[ProgrammableUnitTestEnv]](),
  ): (ContextType, IssConsensusModule[ProgrammableUnitTestEnv]) = {
    implicit val context: ContextType = new ProgrammableUnitTestContext(resolveAwaits)

    implicit val metricsContext: MetricsContext = MetricsContext.Empty

    val dependencies = ConsensusModuleDependencies[ProgrammableUnitTestEnv](
      availabilityModuleRef,
      outputModuleRef,
      p2pNetworkOutModuleRef,
    )

    val latestCompletedEpochFromStore =
      epochStore.latestEpoch(includeInProgress = false)(TraceContext.empty)()
    val latestEpochFromStore =
      epochStore.latestEpoch(includeInProgress = true)(TraceContext.empty)()

    val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

    val initialEpochState =
      preConfiguredInitialEpochState
        .map(_(context))
        .getOrElse {
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
            completedBlocks = completedBlocks,
            loggerFactory = loggerFactory,
            timeouts = timeouts,
          )
        }

    val initialState = IssConsensusModule.InitialState(
      topologyInfo,
      initialEpochState,
      latestCompletedEpochFromStore,
      sequencerSnapshotAdditionalInfo,
    )
    val moduleRefFactory = createSegmentModuleRefFactory(segmentModuleFactoryFunction)

    context ->
      new IssConsensusModule(
        epochLength,
        initialState,
        epochStore,
        clock,
        metrics,
        moduleRefFactory,
        maybeRetransmissionsManager.getOrElse(
          new RetransmissionsManager[ProgrammableUnitTestEnv](
            topologyInfo.thisNode,
            p2pNetworkOutModuleRef,
            fail(_),
            previousEpochsCommitCerts = Map.empty,
            metrics,
            loggerFactory,
          )
        ),
        new Random(4),
        dependencies,
        loggerFactory,
        timeouts,
        futurePbftMessageQueue,
        postponedConsensusMessageQueue,
      )(maybeOnboardingStateTransferManager)(
        catchupDetector = maybeCatchupDetector.getOrElse(
          new DefaultCatchupDetector(topologyInfo.currentMembership, loggerFactory)
        ),
        newEpochTopology = newEpochTopology,
        messageAuthorizer = customMessageAuthorizer.getOrElse(
          topologyInfo.currentMembership.orderingTopology
        ),
      )
  }
}

private[iss] object IssConsensusModuleTest {

  type ContextType =
    ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]]
  val epochLength: EpochLength = DefaultEpochLength
  val aTimestamp: CantonTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))
  val defaultBufferSize = 5
  val myId: BftNodeId = BftNodeId("self")
  val otherIds: IndexedSeq[BftNodeId] = (1 to 3).map { index =>
    BftNodeId(s"node$index")
  }
  val allIds: Seq[BftNodeId] = (myId +: otherIds).sorted
  val aBatchId: BatchId = BatchId.createForTesting("A batch id")
  val oneRequestOrderingBlock: OrderingBlock = OrderingBlock(
    Seq(ProofOfAvailability(aBatchId, Seq.empty, EpochNumber.First))
  )

  private val anOrderingTopology = OrderingTopology.forTesting(allIds.toSet)
  private val aMembership = Membership(myId, anOrderingTopology, allIds)
  private val aFakeCryptoProviderInstance: CryptoProvider[ProgrammableUnitTestEnv] =
    failingCryptoProvider
  private val aTopologyInfo = OrderingTopologyInfo[ProgrammableUnitTestEnv](
    myId,
    anOrderingTopology,
    aFakeCryptoProviderInstance,
    allIds,
    previousTopology = anOrderingTopology, // not relevant
    aFakeCryptoProviderInstance,
    allIds,
  )

  def createSegmentModuleRefFactory(
      segmentModuleFactoryFunction: EpochState.Epoch => ModuleRef[ConsensusSegment.Message]
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
      ): ModuleRef[ConsensusSegment.Message] = segmentModuleFactoryFunction(epoch)
    }

  def orderedBlockFromPrePrepare(prePrepare: PrePrepare): OrderedBlock =
    OrderedBlock(
      prePrepare.blockMetadata,
      prePrepare.block.proofs,
      prePrepare.canonicalCommitSet,
    )
}

final class InMemoryUnitTestEpochStore[E <: BaseIgnoringUnitTestEnv[E]]
    extends GenericInMemoryEpochStore[E]
    with TryValues {

  override protected def createFuture[T](action: String)(
      value: () => Try[T]
  ): E#FutureUnlessShutdownT[T] = () => value().success.value

  override def close(): Unit = ()
}

final class InMemoryUnitTestOutputMetadataStore[E <: BaseIgnoringUnitTestEnv[E]](
    override val loggerFactory: NamedLoggerFactory
) extends GenericInMemoryOutputMetadataStore[E]
    with TryValues
    with NamedLogging {

  override protected def createFuture[T](action: String)(
      value: () => Try[T]
  ): E#FutureUnlessShutdownT[T] = () => value().success.value

  override def close(): Unit = ()

  override protected def reportError(errorMessage: String)(implicit
      traceContext: TraceContext
  ): Unit = logger.error(errorMessage)
}
