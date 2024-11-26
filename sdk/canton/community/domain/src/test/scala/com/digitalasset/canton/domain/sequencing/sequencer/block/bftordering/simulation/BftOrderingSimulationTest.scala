// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.BlockFormat
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.driver.BftBlockOrderer
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability.AvailabilityModuleConfig
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability.data.memory.SimulationAvailabilityStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.SimulationEpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.network.data.memory.SimulationP2pEndpointsStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.OutputModule.RequestInspector
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.memory.SimulationOutputBlockMetadataStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.SimulationBlockSubscription
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.snapshot.{
  PeerActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.{
  SimulationEnv,
  SimulationInitializer,
  SimulationP2PNetworkManager,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.bftordering.{
  BftOrderingVerifier,
  IssClient,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.topology.{
  PeerActiveAtProvider,
  SimulationOrderingTopologyProvider,
  SimulationTopologyHelpers,
}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Random

/** Simulation testing troubleshooting tips & tricks:
  *
  * - When a test fails, it prints the configuration it failed with (including all the seeds that were necessary for the failure).
  *   The configuration can then be copy-pasted one-to-one into the [[generateSimulationSettings]] function of a new or existing test case.
  *   Then, the [[numberOfRuns]] can be lowered down to 1. This narrows the investigation scope and makes logs shorter.
  *   If you take advantage of the logs, remember to remove the log file before investigating such a run.
  *   It can be automated using the command line or in IntelliJ by specifying a "Run external tool" under "Before launch"
  *   in the Run Configurations with "rm" as a program and "log/canton_test.log" as arguments.
  *
  * - Since simulation test runs are fully deterministic, i.e., messages are always processed in the same order,
  *   debugging is significantly easier, e.g., you can use conditional breakpoints with a potentially problematic block number.
  *
  * - Because simulation tests control time and thus are extremely fast, they can be used for profiling and provide performance information.
  *
  * - It's sometimes useful to set a breakpoint somewhere in the [[Simulation]] class to be able to inspect the [[Simulation.currentHistory]].
  *   It should give you an idea of what was happening during the test.
  */
trait BftOrderingSimulationTest extends AnyFlatSpec with BaseTest {

  def numberOfRuns(): Int
  def numberOfInitialPeers(): Int
  // Peers will be onboarded at random points in time during the entire test.
  def numberOfRandomlyOnboardedPeers(): Int
  def generateSimulationSettings(): SimulationSettings

  protected val livenessRecoveryTimeout: FiniteDuration = 10.seconds

  protected val simulationStartTime: CantonTimestamp = CantonTimestamp.Epoch

  private def peers: Seq[String] =
    (0 until numberOfInitialPeers() + numberOfRandomlyOnboardedPeers()).map(i => s"system$i")

  it should "run with no issues" in {
    for (runNumber <- 0 until numberOfRuns()) {
      logger.info(s"Starting run $runNumber (of ${numberOfRuns()})")
      val simSettings = generateSimulationSettings()
      val availabilityRandom = new Random(simSettings.localSettings.randomSeed)

      val initialApplicationHeight = BlockNumber.First // TODO(#17281): Change for restarts/crashes
      val clock = new SimClock(simulationStartTime, loggerFactory)

      val peersWithStores = peers.map { s =>
        Endpoint(s, Port.tryCreate(0)) -> new SimulationOutputBlockMetadataStore
      }
      val allPeerEndpoints = peersWithStores.map(x => x._1)
      val initialPeerEndpoints = allPeerEndpoints.take(numberOfInitialPeers())
      val onboardedPeerEndpoints = allPeerEndpoints.drop(numberOfInitialPeers())
      val peerEndpointsToOnboardingTimes =
        (onboardedPeerEndpoints.zip(simSettings.peerOnboardingTimes) ++
          initialPeerEndpoints.map(_ -> Genesis.GenesisTopologyActivationTime)).toMap

      val peerEndpointsToOnBoardingInformation =
        SimulationTopologyHelpers.generateSimulationOnboardingInformation(
          peerEndpointsToOnboardingTimes,
          loggerFactory,
        )

      implicit val metricsContext: MetricsContext = MetricsContext.Empty
      val noopMetrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

      val sendQueue = mutable.Queue.empty[(SequencerId, BlockFormat.Block)]

      def peer(
          endpoint: Endpoint,
          outputBlockMetadataStore: SimulationOutputBlockMetadataStore,
          initializeImmediately: Boolean,
      ) =
        endpoint -> {

          val peerLogger = loggerFactory.append("peer", s"$endpoint")

          val simulationEpochStore = new SimulationEpochStore()

          val otherInitialEndpoints = initialPeerEndpoints.filterNot(_ == endpoint).toSet

          val stores = BftOrderingStores(
            new SimulationP2pEndpointsStore(otherInitialEndpoints),
            new SimulationAvailabilityStore(),
            simulationEpochStore,
            orderedBlocksReader = simulationEpochStore,
            outputBlockMetadataStore,
          )

          val thisPeer = SimulationP2PNetworkManager.fakeSequencerId(endpoint)
          val orderingTopologyProvider =
            new SimulationOrderingTopologyProvider(
              thisPeer,
              peerEndpointsToOnBoardingInformation,
              loggerFactory,
            )

          val (genesisOrderingTopology, genesisCryptoProvider) =
            SimulationTopologyHelpers.resolveOrderingTopology(
              orderingTopologyProvider.getOrderingTopologyAt(
                TopologyActivationTime(simulationStartTime)
              )
            )

          SimulationInitializer(
            (maybePeerActiveAt: Option[PeerActiveAt]) => {
              val sequencerSnapshotAdditionalInfo =
                if (genesisOrderingTopology.contains(thisPeer))
                  None
                else {
                  // Non-simulated snapshots contain information about other peers as well.
                  //  We skip it here for simplicity. Fully reflecting the non-simulated logic would be pointless,
                  //  as it would still be totally different code.
                  maybePeerActiveAt.map(peerActiveAt =>
                    SequencerSnapshotAdditionalInfo(Map(thisPeer -> peerActiveAt))
                  )
                }
              logger.info(
                s"Sequencer snapshot additional info for $thisPeer: $sequencerSnapshotAdditionalInfo"
              )

              val (initialOrderingTopology, initialCryptoProvider) =
                if (genesisOrderingTopology.contains(thisPeer))
                  (genesisOrderingTopology, genesisCryptoProvider)
                else {
                  // We keep the onboarding topology after state transfer. See the Output module for details.
                  maybePeerActiveAt
                    .flatMap(_.timestamp)
                    .map(onboardingTime =>
                      SimulationTopologyHelpers.resolveOrderingTopology(
                        orderingTopologyProvider.getOrderingTopologyAt(onboardingTime)
                      )
                    )
                    .getOrElse((genesisOrderingTopology, genesisCryptoProvider))
                }

              // Forces always querying for an up-to-date topology, so that we simulate correctly topology changes.
              val requestInspector: RequestInspector =
                (_: OrderingRequest, _: ProtocolVersion, _: TracedLogger, _: TraceContext) => true

              BftOrderingModuleSystemInitializer[SimulationEnv](
                thisPeer,
                testedProtocolVersion,
                initialOrderingTopology,
                initialCryptoProvider,
                BftBlockOrderer.Config(),
                initialApplicationHeight,
                IssConsensusModule.DefaultEpochLength,
                stores,
                orderingTopologyProvider,
                new SimulationBlockSubscription(thisPeer, sendQueue),
                sequencerSnapshotAdditionalInfo,
                clock,
                availabilityRandom,
                noopMetrics,
                peerLogger,
                timeouts,
                requestInspector,
              )
            },
            IssClient.initializer(simSettings.clientRequestInterval, peerLogger, timeouts),
            initializeImmediately,
          )
        }

      val topologyInitializers = peersWithStores.zipWithIndex.map {
        case ((endpoint, store), index) =>
          peer(endpoint, store, initializeImmediately = index < numberOfInitialPeers())
      }.toMap

      val stores = peersWithStores.view.map { case (endpoint, store) =>
        SimulationP2PNetworkManager.fakeSequencerId(endpoint) -> store
      }.toMap
      val peersWithOnboardingTimes = peerEndpointsToOnboardingTimes.view.map {
        case (endpoint, onboardingTime) =>
          SimulationP2PNetworkManager.fakeSequencerId(endpoint) -> onboardingTime
      }.toMap
      val simulation =
        SimulationModuleSystem(
          topologyInitializers,
          new PeerActiveAtProvider(peersWithOnboardingTimes, stores),
          simSettings,
          clock,
          timeouts,
          loggerFactory,
        )

      val model = new BftOrderingVerifier(
        sendQueue,
        stores,
        peersWithOnboardingTimes,
        livenessRecoveryTimeout,
      )

      simulation.run(model)
    }
  }
}

class BftOrderingSimulationTest1NodeNoFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 10
  override val numberOfInitialPeers: Int = 1
  override val numberOfRandomlyOnboardedPeers: Int = 0

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateSimulationSettings(): SimulationSettings =
    SimulationSettings(
      LocalSettings(
        randomSeed = randomSourceToCreateSettings.nextLong()
      ),
      NetworkSettings(
        randomSeed = randomSourceToCreateSettings.nextLong()
      ),
      durationOfFirstPhaseWithFaults,
      durationOfSecondPhaseWithoutFaults,
    )
}

class BftOrderingSimulationTest2NodesWithOnboardingNoFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 10
  override val numberOfInitialPeers: Int = 2
  override val numberOfRandomlyOnboardedPeers: Int = 1

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateSimulationSettings(): SimulationSettings =
    SimulationSettings(
      LocalSettings(
        randomSeed = randomSourceToCreateSettings.nextLong()
      ),
      NetworkSettings(
        randomSeed = randomSourceToCreateSettings.nextLong()
      ),
      durationOfFirstPhaseWithFaults,
      durationOfSecondPhaseWithoutFaults,
      peerOnboardingTimes = LazyList
        .iterate(
          generatePeerOnboardingTime(TopologyActivationTime(simulationStartTime)),
          numberOfRandomlyOnboardedPeers,
        )(
          generatePeerOnboardingTime
        ),
    )

  private def generatePeerOnboardingTime(
      initialTime: TopologyActivationTime
  ): TopologyActivationTime = {
    val initialTimeEpochMilli = initialTime.value.toEpochMilli
    val simulationEndTimeEpochMilli = simulationStartTime
      .plus(durationOfFirstPhaseWithFaults.toJava)
      .plus(durationOfSecondPhaseWithoutFaults.toJava)
      .toEpochMilli
    val onboardingTimeEpochMilli =
      initialTimeEpochMilli + randomSourceToCreateSettings.nextLong(
        simulationEndTimeEpochMilli - initialTimeEpochMilli
      )
    TopologyActivationTime(CantonTimestamp.ofEpochMilli(onboardingTimeEpochMilli))
  }
}

class BftOrderingSimulationTest2NodesBootstrap extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 100
  override val numberOfInitialPeers: Int = 2
  override val numberOfRandomlyOnboardedPeers: Int = 0

  private val durationOfFirstPhaseWithFaults = 2.seconds
  private val durationOfSecondPhaseWithoutFaults = 2.seconds

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateSimulationSettings(): SimulationSettings =
    SimulationSettings(
      LocalSettings(
        randomSeed = randomSourceToCreateSettings.nextLong()
      ),
      NetworkSettings(
        randomSeed = randomSourceToCreateSettings.nextLong()
      ),
      durationOfFirstPhaseWithFaults,
      durationOfSecondPhaseWithoutFaults,
    )
}

// Simulation test about empty blocks, needed to pass the liveness check.
class BftOrderingEmptyBlocksSimulationTest extends BftOrderingSimulationTest {
  // At the moment of writing, the test requires 12 runs to fail on the liveness check when there's no "silent network detection".
  override val numberOfRuns: Int = 15
  override val numberOfInitialPeers: Int = 2
  override val numberOfRandomlyOnboardedPeers: Int = 0

  // This value is lower than the default to prevent view changes from ensuring liveness (as we want empty blocks to ensure it).
  // When the simulation becomes "healthy", we don't know when the last crash (resetting the view change timeout)
  // or view change happened. Similarly, we don't know how "advanced" the empty block creation at that moment is.
  // Since the simulation is deterministic and runs multiple times, we can base this value on the empty block creation
  // interval to get the desired test coverage.
  protected override val livenessRecoveryTimeout: FiniteDuration =
    AvailabilityModuleConfig.EmptyBlockCreationInterval * 2 + 1.second

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random = new Random(4)

  override def generateSimulationSettings(): SimulationSettings =
    SimulationSettings(
      LocalSettings(
        randomSeed = randomSourceToCreateSettings.nextLong()
      ),
      NetworkSettings(
        randomSeed = randomSourceToCreateSettings.nextLong()
      ),
      durationOfFirstPhaseWithFaults,
      durationOfSecondPhaseWithoutFaults,
      // This will result in empty blocks only.
      clientRequestInterval = None,
    )
}

/*
// TODO(#17284) Activate when we can handle the crash restart fault
class BftOrderingSimulationTest2NodesCrashFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 10
  override val numberOfPeers: Int = 2

  private val randomSourceToCreateSettings: Random = new Random(4) // remove seed to randomly explore seeds

  override def generateSimulationSettings(): SimulationSettings = SimulationSettings(
    localSettings = LocalSettings(
      randomSeed = randomSourceToCreateSettings.nextLong(),
      crashRestartChance = Probability(0.01),
    ),
      randomSeed = randomSourceToCreateSettings.nextLong()
    ),
    durationWithFaults = 2.minutes,
  )
}
 */
