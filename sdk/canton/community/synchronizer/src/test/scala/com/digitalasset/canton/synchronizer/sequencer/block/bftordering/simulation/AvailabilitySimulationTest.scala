// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{ProcessingTimeout, TlsClientConfig}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrderer.{
  P2PEndpointConfig,
  P2PNetworkConfig,
  P2PServerConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.memory.SimulationAvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.network.data.memory.SimulationP2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.{
  BftP2PNetworkIn,
  BftP2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.{
  SystemInitializationResult,
  SystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  OrderingBlock,
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
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.{
  AvailabilityModuleDependencies,
  ConsensusModuleDependencies,
  P2PNetworkOutModuleDependencies,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.{
  SimulationEnv,
  SimulationInitializer,
  SimulationP2PNetworkManager,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.EmptyOnboardingDataProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.{
  SimulationOrderingTopologyProvider,
  SimulationTopologyHelpers,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveRequest
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.Random

class AvailabilitySimulationTest extends AnyFlatSpec with BaseTest {

  private val RandomSeed = 4L
  private val SimulationVirtualDuration = 2.minutes

  private val MaxRequestsInBatch: Short = 4
  private val MaxBatchesPerProposal: Short = 4

  private val ProposalRequestsPerPeer = 16
  private val RequestsPerPeer =
    ProposalRequestsPerPeer * MaxRequestsInBatch * MaxBatchesPerProposal

  private class SimulationModel {
    var requestIndex = 0
    val proposalsToConsensus: mutable.ArrayBuffer[Consensus.LocalAvailability.ProposalCreated] =
      mutable.ArrayBuffer()
    val fetchedOutputBlocks: mutable.ArrayBuffer[CompleteBlockData] = mutable.ArrayBuffer()

    val availabilityStorage: mutable.SortedMap[BatchId, OrderingRequestBatch] =
      mutable.SortedMap()

    val disseminationProtocolState: DisseminationProtocolState = new DisseminationProtocolState()
    val mainOutputFetchProtocolState: MainOutputFetchProtocolState =
      new MainOutputFetchProtocolState
  }

  class MempoolSimulationFake[E <: Env[E]](
      simulationModel: SimulationModel,
      selfPeer: SequencerId,
      override val availability: ModuleRef[Availability.Message[E]],
      override val loggerFactory: NamedLoggerFactory,
      override val timeouts: ProcessingTimeout,
  ) extends Mempool[E]
      with NamedLogging {

    override def ready(self: ModuleRef[Mempool.Message]): Unit =
      (1 to RequestsPerPeer).foreach { _ =>
        val batch =
          OrderingRequestBatch.create(
            Seq(
              Traced(
                OrderingRequest(
                  "tx",
                  ByteString.copyFromUtf8(f"$selfPeer-request-${simulationModel.requestIndex}"),
                )
              )(
                TraceContext.empty
              )
            )
          )
        val request = Availability.LocalDissemination.LocalBatchCreated(BatchId.from(batch), batch)
        simulationModel.requestIndex += 1
        availability.asyncSend(request)
      }

    override def receiveInternal(
        msg: Mempool.Message
    )(implicit context: E#ActorContextT[Mempool.Message], traceContext: TraceContext): Unit = {}
  }

  class ConsensusSimulationFake[E <: Env[E]](
      membership: Membership,
      simulationModel: SimulationModel,
      cryptoProvider: CryptoProvider[E],
      override val dependencies: ConsensusModuleDependencies[E],
      override val loggerFactory: NamedLoggerFactory,
      override val timeouts: ProcessingTimeout,
  ) extends Consensus[E]
      with NamedLogging {

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var proposalsRequested = false

    override def receiveInternal(
        msg: Consensus.Message[E]
    )(implicit
        context: E#ActorContextT[Consensus.Message[E]],
        traceContext: TraceContext,
    ): Unit =
      msg match {
        case Consensus.Start =>
          if (!proposalsRequested) {
            logger.info("Requesting proposals as all peers are authenticated")
            (1 to ProposalRequestsPerPeer).foreach { _ =>
              dependencies.availability.asyncSend(
                Availability.Consensus
                  .CreateProposal(
                    membership.orderingTopology,
                    cryptoProvider,
                    Genesis.GenesisEpochInfo.number,
                  )
              )
            }
            proposalsRequested = true
          }

        case proposal @ Consensus.LocalAvailability.ProposalCreated(OrderingBlock(batches), _) =>
          if (proposalsRequested) {
            simulationModel.proposalsToConsensus.addOne(proposal)
            dependencies.output.asyncSend(
              Output.BlockOrdered(
                OrderedBlockForOutput(
                  OrderedBlock(
                    BlockMetadata.mk(
                      epochNumber = EpochNumber.First,
                      blockNumber = simulationModel.proposalsToConsensus.size.toLong - 1,
                    ),
                    batches,
                    CanonicalCommitSet(Set.empty),
                  ),
                  membership.myId,
                  isLastInEpoch = false, // Irrelevant for availability
                  OrderedBlockForOutput.Mode.FromConsensus,
                )
              )
            )
          } else {
            abort("Proposal received before being requested")
          }

        case unexpectedMessage =>
          abort(s"Unexpected message type for consensus module: $unexpectedMessage")
      }
  }

  class OutputSimulationFake[E <: Env[E]](
      simulationModel: SimulationModel,
      override val availability: ModuleRef[Availability.Message[E]],
      override val consensus: ModuleRef[Consensus.Message[E]],
      override val loggerFactory: NamedLoggerFactory,
  ) extends Output[E]
      with NamedLogging {

    override def receiveInternal(
        msg: Output.Message[E]
    )(implicit context: E#ActorContextT[Output.Message[E]], traceContext: TraceContext): Unit =
      msg match {
        case Output.BlockDataFetched(blockData) =>
          simulationModel.fetchedOutputBlocks.addOne(blockData)
        case Output.BlockOrdered(orderedBlockForOutput) =>
          availability.asyncSend(
            Availability.LocalOutputFetch.FetchBlockData(orderedBlockForOutput)
          )
        case _ => ()
      }

    override protected def timeouts: ProcessingTimeout = ProcessingTimeout()
  }

  private def availabilityOnlySystemInitializer(
      selfPeer: SequencerId,
      config: BftBlockOrderer.Config,
      random: Random,
      simulationModel: SimulationModel,
      cryptoProvider: CryptoProvider[SimulationEnv],
      clock: Clock,
      store: mutable.Map[BatchId, OrderingRequestBatch] => AvailabilityStore[SimulationEnv],
  ): SystemInitializer[
    SimulationEnv,
    BftOrderingServiceReceiveRequest,
    Availability.LocalDissemination.LocalBatchCreated,
  ] = (moduleSystem, p2pNetworkManager) => {
    val loggerFactoryWithSequencerId = loggerFactory.append("sequencerId", selfPeer.toString)

    val mempoolRef = moduleSystem.newModuleRef[Mempool.Message](ModuleName("mempool"))
    val p2pNetworkInRef = moduleSystem
      .newModuleRef[BftOrderingServiceReceiveRequest](ModuleName("p2p-network-in"))
    val p2pNetworkOutRef = moduleSystem
      .newModuleRef[P2PNetworkOut.Message](ModuleName("p2p-network-out"))
    val availabilityRef = moduleSystem
      .newModuleRef[Availability.Message[SimulationEnv]](ModuleName("availability"))
    val consensusRef = moduleSystem
      .newModuleRef[Consensus.Message[SimulationEnv]](ModuleName("consensus"))
    val outputRef = moduleSystem
      .newModuleRef[Output.Message[SimulationEnv]](ModuleName("output"))

    val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
    implicit val metricsContext: MetricsContext = MetricsContext.Empty

    val mempoolSimulationFake =
      new MempoolSimulationFake[SimulationEnv](
        simulationModel,
        selfPeer,
        availabilityRef,
        loggerFactoryWithSequencerId,
        timeouts,
      )
    val p2pNetworkIn =
      new BftP2PNetworkIn[SimulationEnv](
        metrics,
        availabilityRef,
        consensusRef,
        loggerFactoryWithSequencerId,
        timeouts,
      )
    val p2PNetworkOutDependencies = P2PNetworkOutModuleDependencies(
      p2pNetworkManager,
      p2pNetworkInRef,
      mempoolRef,
      availabilityRef,
      consensusRef,
      outputRef,
    )
    val p2pNetworkOut =
      new BftP2PNetworkOut[SimulationEnv](
        selfPeer,
        new SimulationP2PEndpointsStore(
          config.initialNetwork
            .map(_.peerEndpoints.map(P2PEndpoint.fromEndpointConfig))
            .getOrElse(Seq.empty)
            .toSet
        ),
        metrics,
        p2PNetworkOutDependencies,
        loggerFactoryWithSequencerId,
        timeouts,
      )
    val peerSequencerIds = config.initialNetwork.toList
      .flatMap(_.peerEndpoints.map(P2PEndpoint.fromEndpointConfig))
      .map(SimulationP2PNetworkManager.fakeSequencerId)
    val membership = Membership(selfPeer, peerSequencerIds.toSet)
    val availabilityStore = store(simulationModel.availabilityStorage)
    val availabilityConfig = AvailabilityModuleConfig(
      config.maxRequestsInBatch,
      config.maxBatchesPerBlockProposal,
      config.outputFetchTimeout,
    )
    val availabilityDependencies = AvailabilityModuleDependencies(
      mempoolRef,
      p2pNetworkOutRef,
      consensusRef,
      outputRef,
    )
    val availability = new AvailabilityModule[SimulationEnv](
      membership,
      cryptoProvider,
      availabilityStore,
      availabilityConfig,
      clock,
      random,
      metrics,
      availabilityDependencies,
      loggerFactoryWithSequencerId,
      timeouts,
      simulationModel.disseminationProtocolState,
      simulationModel.mainOutputFetchProtocolState,
    )
    val consensusDependencies = ConsensusModuleDependencies(
      availabilityRef,
      outputRef,
      p2pNetworkOutRef,
    )
    val consensusSimulationFake =
      new ConsensusSimulationFake[SimulationEnv](
        membership,
        simulationModel,
        cryptoProvider,
        consensusDependencies,
        loggerFactoryWithSequencerId,
        timeouts,
      )
    val outputSimulationFake =
      new OutputSimulationFake[SimulationEnv](
        simulationModel,
        availabilityRef,
        consensusRef,
        loggerFactoryWithSequencerId,
      )

    moduleSystem.setModule(mempoolRef, mempoolSimulationFake)
    moduleSystem.setModule(p2pNetworkInRef, p2pNetworkIn)
    moduleSystem.setModule(p2pNetworkOutRef, p2pNetworkOut)
    moduleSystem.setModule(availabilityRef, availability)
    moduleSystem.setModule(consensusRef, consensusSimulationFake)
    moduleSystem.setModule(outputRef, outputSimulationFake)

    mempoolSimulationFake.ready(mempoolRef)
    p2pNetworkOut.ready(p2pNetworkOutRef)

    SystemInitializationResult(
      availabilityRef,
      p2pNetworkInRef,
      p2pNetworkOutRef,
      consensusRef,
      outputRef,
    )
  }

  it should "run with no issues" in {
    val simSettings = SimulationSettings(
      LocalSettings(RandomSeed),
      NetworkSettings(RandomSeed),
      SimulationVirtualDuration,
    )

    forAll(
      Table("Peers count", 1, 2, 3, 4)
    ) { peersCount =>
      val peersRange: Range = 0 until peersCount
      val peerEndpoints = peersRange.map(n =>
        P2PEndpointConfig(
          s"peer$n",
          Port.tryCreate(0),
          Some(TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = false)),
        )
      )
      val configs =
        peerEndpoints.map { peer =>
          BftBlockOrderer.Config(
            initialNetwork = Some(
              P2PNetworkConfig(
                P2PServerConfig(peer.address, Some(peer.port)),
                peerEndpoints.filterNot(_ == peer),
              )
            ),
            maxRequestsInBatch = MaxRequestsInBatch,
            maxBatchesPerBlockProposal = MaxBatchesPerProposal,
          )
        }
      val availabilityQuorum = AvailabilityModule.quorum(peersCount)
      val minimumNumberOfCorrectNodes = OrderingTopology.strongQuorumSize(peersCount)

      val simulationModels = peersRange.map(_ => new SimulationModel).toArray
      val clock = new SimClock(loggerFactory = loggerFactory)

      val peerEndpointsToOnboardingTimes = peerEndpoints.map { endpoint =>
        P2PEndpoint.fromEndpointConfig(
          endpoint
        ) -> Genesis.GenesisTopologyActivationTime
      }.toMap

      val peerEndpointsSimulationTopologyData =
        SimulationTopologyHelpers.generateSimulationTopologyData(
          peerEndpointsToOnboardingTimes,
          loggerFactory,
        )

      val topologyInit = peersRange.map { n =>
        val peerEndpointConfig = peerEndpoints(n)
        val peerEndpoint = PlainTextP2PEndpoint(peerEndpointConfig.address, peerEndpointConfig.port)
        val sequencerId = SimulationP2PNetworkManager.fakeSequencerId(peerEndpoint)

        val orderingTopologyProvider =
          new SimulationOrderingTopologyProvider(
            sequencerId,
            () => peerEndpointsSimulationTopologyData,
            loggerFactory,
          )
        val (_, cryptoProvider) = SimulationTopologyHelpers.resolveOrderingTopology(
          orderingTopologyProvider.getOrderingTopologyAt(Genesis.GenesisTopologyActivationTime)
        )

        peerEndpoint -> SimulationInitializer.noClient[
          BftOrderingServiceReceiveRequest,
          Availability.LocalDissemination.LocalBatchCreated,
          Unit,
        ](loggerFactory, timeouts)(
          availabilityOnlySystemInitializer(
            sequencerId,
            configs(n),
            new Random(n),
            simulationModels(n),
            cryptoProvider,
            clock,
            xs => new SimulationAvailabilityStore(xs),
          )
        )
      }.toMap

      val simulation =
        SimulationModuleSystem(
          topologyInit,
          EmptyOnboardingDataProvider,
          simSettings,
          clock,
          timeouts,
          loggerFactory,
        )

      // Run and check invariants

      simulation.run {
        SimulationVerifier.onlyCheckInvariant { _ =>
          simulationModels.forall { simulationModel =>
            simulationModel.proposalsToConsensus.forall { proposal =>
              proposal.orderingBlock.proofs.forall { proofOfAvailability =>
                simulationModels.count { simulationModel =>
                  simulationModel.availabilityStorage.contains(proofOfAvailability.batchId) &&
                  proofOfAvailability.acks.sizeIs >= availabilityQuorum
                } >= availabilityQuorum
              }
            }
          } shouldBe true
        }
      }

      simulationModels.count { simulationModel =>
        simulationModel.availabilityStorage.keys.toSet.sizeIs >= RequestsPerPeer * availabilityQuorum
      } should be >= minimumNumberOfCorrectNodes

      simulationModels.count { simulationModel =>
        simulationModel.proposalsToConsensus.sizeIs == ProposalRequestsPerPeer
      } should be >= minimumNumberOfCorrectNodes

      simulationModels.count { simulationModel =>
        simulationModel.proposalsToConsensus.forall { proposal =>
          val proposalBatchesCount = proposal.orderingBlock.proofs.size
          proposalBatchesCount > 0 && proposalBatchesCount <= MaxBatchesPerProposal
        }
      } should be >= minimumNumberOfCorrectNodes

      simulationModels.count { simulationModel =>
        simulationModel.fetchedOutputBlocks.sizeIs == ProposalRequestsPerPeer
      } should be >= minimumNumberOfCorrectNodes

      simulationModels.count { simulationModel =>
        simulationModel.fetchedOutputBlocks.forall { blockData =>
          val blockBatchesCount = blockData.batches.size
          blockBatchesCount > 0 && blockBatchesCount <= MaxBatchesPerProposal
        }
      } should be >= minimumNumberOfCorrectNodes

      simulationModels.count { simulationModel =>
        simulationModel.proposalsToConsensus.forall { proposalToConsensus =>
          proposalToConsensus.orderingBlock.proofs.nonEmpty &&
          proposalToConsensus.orderingBlock.proofs.forall { proof =>
            simulationModels.count { model =>
              model.availabilityStorage.contains(proof.batchId)
            } >= availabilityQuorum
          }
        }
      } should be >= minimumNumberOfCorrectNodes
    }
  }
}
