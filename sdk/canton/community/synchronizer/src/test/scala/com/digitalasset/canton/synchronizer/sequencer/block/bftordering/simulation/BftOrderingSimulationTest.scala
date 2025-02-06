// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModuleConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.memory.SimulationAvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.SimulationEpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.network.data.memory.SimulationP2pEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.RequestInspector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory.SimulationOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.SimulationBlockSubscription
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.{
  PeerActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Mempool
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.{
  SimulationEnv,
  SimulationInitializer,
  SimulationP2PNetworkManager,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.BftOrderingSimulationTest.{
  SimulationStartTime,
  SimulationTestStage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering.{
  BftOrderingVerifier,
  IssClient,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.SimulationTopologyHelpers.{
  generatePeerOnboardingDelay,
  onboardingTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.{
  PeerActiveAtProvider,
  SimulationOrderingTopologyProvider,
  SimulationTopologyData,
  SimulationTopologyHelpers,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1.BftOrderingServiceReceiveRequest
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

/** Simulation testing troubleshooting tips & tricks:
  *
  * - When a test fails, it prints the configuration it failed with (including all the seeds that were necessary for the failure).
  *   The configuration can then be copy-pasted one-to-one into [[generateStages]] of a new or existing test case.
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

  def numberOfRuns: Int
  def numberOfInitialPeers: Int
  def generateStages(): Seq[SimulationTestStage]

  private type SimulationT = Simulation[Option[
    PeerActiveAt
  ], BftOrderingServiceReceiveRequest, Mempool.Message, Unit]

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty
  private val noopMetrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

  private val initialApplicationHeight =
    BlockNumber.First // TODO(#17281): Change for restarts/crashes

  private lazy val initialPeersIndexRange = 0 until numberOfInitialPeers
  private lazy val initialPeersOnboardingTimes =
    initialPeersIndexRange.map(_ => Genesis.GenesisTopologyActivationTime)
  private lazy val initialPeerEndpoints =
    initialPeersIndexRange.map(i => Endpoint(peerHostname(i), Port.tryCreate(0)))
  private lazy val initialPeerEndpointsWithOnboardingTimes =
    initialPeerEndpoints.zip(initialPeersOnboardingTimes)
  private lazy val initialPeerEndpointsToTopologyData =
    SimulationTopologyHelpers.generateSimulationTopologyData(
      initialPeerEndpointsWithOnboardingTimes.toMap,
      loggerFactory,
    )
  private lazy val initialSequencerIdsToOnboardingTimes =
    initialPeerEndpoints
      .zip(initialPeersOnboardingTimes)
      .view
      .map { case (endpoint, onboardingTime) =>
        SimulationP2PNetworkManager.fakeSequencerId(endpoint) -> onboardingTime
      }
      .toMap

  private def peerHostname(i: Int) = s"system$i"

  it should "run with no issues" in {

    for (runNumber <- 1 to numberOfRuns) {

      logger.info(s"Starting run $runNumber (of $numberOfRuns)")

      val initialPeersWithStores =
        initialPeerEndpoints.map(_ -> new SimulationOutputMetadataStore(fail(_))).toMap
      val initialSequencerIdsToStores =
        initialPeersWithStores.view.map { case (endpoint, store) =>
          SimulationP2PNetworkManager.fakeSequencerId(endpoint) -> store
        }.toMap

      val sendQueue = mutable.Queue.empty[(SequencerId, BlockFormat.Block)]

      var firstNewlyOnboardedPeerIndex = initialPeerEndpoints.size

      var alreadyOnboardedPeerEndpoints = initialPeerEndpoints
      var alreadyOnboardedPeerEndpointsToTopologyData = initialPeerEndpointsToTopologyData
      var alreadyOnboardedSequencerIdsToOnboardingTimes = initialSequencerIdsToOnboardingTimes
      var alreadyOnboardedSequencerIdsToStores = initialSequencerIdsToStores
      var simulationAndModel: Option[(SimulationT, BftOrderingVerifier)] = None

      val clock = new SimClock(SimulationStartTime, loggerFactory)

      val allEndpointsToTopologyDataCell =
        new AtomicReference[Map[Endpoint, SimulationTopologyData]](Map.empty)
      def getAllEndpointsToTopologyData = allEndpointsToTopologyDataCell.get()

      val stages = generateStages()
      val stagesCount = stages.size
      stages.zipWithIndex.foreach {
        case (SimulationTestStage(numberOfRandomlyOnboardedPeers, simSettings), idx) =>
          val stageStart = clock.now

          logger.info(s"Starting stage ${idx + 1} (of $stagesCount) at $stageStart")

          val availabilityRandom = new Random(simSettings.localSettings.randomSeed)

          val newlyOnboardedPeerEndpoints =
            (firstNewlyOnboardedPeerIndex until firstNewlyOnboardedPeerIndex + numberOfRandomlyOnboardedPeers)
              .map(i => Endpoint(peerHostname(i), Port.tryCreate(0)))
          val newlyOnboardedPeersWithStores =
            newlyOnboardedPeerEndpoints.map(_ -> new SimulationOutputMetadataStore(fail(_)))
          val newlyOnboardedPeerEndpointsWithOnboardingTimes =
            newlyOnboardedPeerEndpoints.zip(
              simSettings.peerOnboardingDelays.map(onboardingTime(stageStart, _))
            )

          val newlyOnboardedPeerEndpointsToTopologyData =
            SimulationTopologyHelpers.generateSimulationTopologyData(
              newlyOnboardedPeerEndpointsWithOnboardingTimes.toMap,
              loggerFactory,
            )
          val allEndpointsToTopologyData =
            alreadyOnboardedPeerEndpointsToTopologyData ++ newlyOnboardedPeerEndpointsToTopologyData

          allEndpointsToTopologyDataCell.set(allEndpointsToTopologyData)

          def peerInitializer(
              endpoint: Endpoint,
              store: SimulationOutputMetadataStore,
              initializeImmediately: Boolean,
          ) =
            newPeerInitializer(
              endpoint,
              alreadyOnboardedPeerEndpoints,
              () => getAllEndpointsToTopologyData,
              store,
              sendQueue,
              clock,
              availabilityRandom,
              simSettings,
              initializeImmediately,
            )

          val newlyOnboardedTopologyInitializers =
            newlyOnboardedPeersWithStores.map { case (endpoint, store) =>
              endpoint -> peerInitializer(
                endpoint,
                store,
                initializeImmediately = false,
              )
            }.toMap
          val newlyOnboardedSequencerIdsToOnboardingTimes =
            newlyOnboardedPeerEndpointsWithOnboardingTimes.view.map {
              case (endpoint, onboardingTime) =>
                SimulationP2PNetworkManager.fakeSequencerId(endpoint) -> onboardingTime
            }.toMap
          val newlyOnboardedSequencerIdsToStores =
            newlyOnboardedPeersWithStores.view.map { case (endpoint, store) =>
              SimulationP2PNetworkManager.fakeSequencerId(endpoint) -> store
            }.toMap

          val allSequencerIdsToOnboardingTimes =
            alreadyOnboardedSequencerIdsToOnboardingTimes ++ newlyOnboardedSequencerIdsToOnboardingTimes
          val allSequencerIdsToStores =
            alreadyOnboardedSequencerIdsToStores ++ newlyOnboardedSequencerIdsToStores

          simulationAndModel = simulationAndModel match {
            case None => // First stage
              val initialTopologyInitializers =
                initialPeersWithStores.map { case (endpoint, store) =>
                  endpoint -> peerInitializer(
                    endpoint,
                    store,
                    initializeImmediately = true,
                  )
                }
              val allTopologyInitializers =
                initialTopologyInitializers ++ newlyOnboardedTopologyInitializers
              val simulation =
                SimulationModuleSystem(
                  allTopologyInitializers,
                  new PeerActiveAtProvider(
                    allSequencerIdsToOnboardingTimes,
                    allSequencerIdsToStores,
                  ),
                  simSettings,
                  clock,
                  timeouts,
                  loggerFactory,
                )
              val model =
                new BftOrderingVerifier(
                  sendQueue,
                  allSequencerIdsToStores,
                  allSequencerIdsToOnboardingTimes,
                  simSettings,
                )
              Some(simulation -> model)

            case Some((previousSimulation, previousModel)) => // Subsequent stages
              Some(
                previousSimulation.newStage(
                  simSettings,
                  new PeerActiveAtProvider(
                    allSequencerIdsToOnboardingTimes,
                    allSequencerIdsToStores,
                  ),
                  newlyOnboardedTopologyInitializers,
                ) ->
                  previousModel.newStage(
                    simSettings,
                    newlyOnboardedSequencerIdsToOnboardingTimes,
                    newlyOnboardedSequencerIdsToStores,
                  )
              )
          }

          val (simulation, model) =
            simulationAndModel
              .getOrElse(fail("The simulation object was not set but it should always be"))

          simulation.run(model)

          // Prepare for next stage
          firstNewlyOnboardedPeerIndex += numberOfRandomlyOnboardedPeers
          alreadyOnboardedPeerEndpoints ++= newlyOnboardedPeerEndpoints
          alreadyOnboardedPeerEndpointsToTopologyData = allEndpointsToTopologyData
          alreadyOnboardedSequencerIdsToOnboardingTimes = allSequencerIdsToOnboardingTimes
          alreadyOnboardedSequencerIdsToStores = allSequencerIdsToStores
      }
    }
  }

  def newPeerInitializer(
      endpoint: Endpoint,
      alreadyOnboardedPeerEnpoints: Iterable[Endpoint],
      getAllEndpointsToTopologyData: () => Map[Endpoint, SimulationTopologyData],
      outputBlockMetadataStore: SimulationOutputMetadataStore,
      sendQueue: mutable.Queue[(SequencerId, BlockFormat.Block)],
      clock: Clock,
      availabilityRandom: Random,
      simSettings: SimulationSettings,
      initializeImmediately: Boolean,
  ): SimulationInitializer[Option[
    PeerActiveAt
  ], BftOrderingServiceReceiveRequest, Mempool.Message, Unit] = {

    val peerLogger = loggerFactory.append("peer", s"$endpoint")

    val simulationEpochStore = new SimulationEpochStore()

    val otherInitialEndpoints = alreadyOnboardedPeerEnpoints.filterNot(_ == endpoint).toSet

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
        getAllEndpointsToTopologyData,
        loggerFactory,
      )

    val (genesisTopology, genesisCryptoProvider) =
      SimulationTopologyHelpers.resolveOrderingTopology(
        orderingTopologyProvider.getOrderingTopologyAt(
          TopologyActivationTime(SimulationStartTime)
        )
      )

    SimulationInitializer(
      (maybePeerActiveAt: Option[PeerActiveAt]) => {
        val sequencerSnapshotAdditionalInfo =
          if (genesisTopology.contains(thisPeer))
            None
          else {
            // Non-simulated snapshots contain information about other peers as well.
            //  We skip it here for simplicity. Fully reflecting the non-simulated logic would be pointless,
            //  as it would still be totally different code.
            maybePeerActiveAt.map(peerActiveAt =>
              SequencerSnapshotAdditionalInfo(Map(thisPeer -> peerActiveAt))
            )
          }

        val genesisTopologyInfo = OrderingTopologyInfo(
          thisPeer,
          currentTopology = genesisTopology,
          currentCryptoProvider = genesisCryptoProvider,
          previousTopology = genesisTopology,
          previousCryptoProvider = genesisCryptoProvider,
        )
        val bootstrapTopologyInfo =
          if (genesisTopology.contains(thisPeer))
            genesisTopologyInfo
          else {
            maybePeerActiveAt
              .flatMap(_.timestamp)
              .map { onboardingTime =>
                val (initialOrderingTopology, initialCryptoProvider) =
                  SimulationTopologyHelpers.resolveOrderingTopology(
                    orderingTopologyProvider.getOrderingTopologyAt(onboardingTime)
                  )
                val (previousOrderingTopology, previousCryptoProvider) =
                  SimulationTopologyHelpers.resolveOrderingTopology(
                    orderingTopologyProvider.getOrderingTopologyAt(
                      // get the topology from just before the onboarding
                      // TODO(#23659) try to unify with non-simulated code
                      TopologyActivationTime(onboardingTime.value.immediatePredecessor)
                    )
                  )
                OrderingTopologyInfo(
                  thisPeer,
                  initialOrderingTopology,
                  initialCryptoProvider,
                  previousOrderingTopology,
                  previousCryptoProvider,
                )
              }
              .getOrElse(genesisTopologyInfo)
          }

        // Forces always querying for an up-to-date topology, so that we simulate correctly topology changes.
        val requestInspector: RequestInspector =
          (_: OrderingRequest, _: ProtocolVersion, _: TracedLogger, _: TraceContext) => true

        BftOrderingModuleSystemInitializer[SimulationEnv](
          testedProtocolVersion,
          bootstrapTopologyInfo,
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
      IssClient.initializer(simSettings.clientRequestInterval, thisPeer, peerLogger, timeouts),
      initializeImmediately,
    )
  }
}

object BftOrderingSimulationTest {

  val SimulationStartTime: CantonTimestamp = CantonTimestamp.Epoch

  final case class SimulationTestStage(
      // Peers will be onboarded after delays defined in settings, during the portion of the stage in which faults may
      //  also happen.
      numberOfOnboardedPeers: Int,
      simulationSettings: SimulationSettings,
  )
}

class BftOrderingSimulationTest1NodeNoFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 10
  override val numberOfInitialPeers: Int = 1

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateStages(): Seq[SimulationTestStage] = Seq(
    SimulationTestStage(
      numberOfOnboardedPeers = 0,
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
      ),
    )
  )
}

class BftOrderingSimulationTestWithProgressiveOnboardingAndDelayNoFaults
    extends BftOrderingSimulationTest {

  override val numberOfRuns: Int = 3

  override val numberOfInitialPeers: Int = 1

  private val numberOfRandomlyOnboardedPeers = 1

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateStages(): Seq[SimulationTestStage] = {
    val stagesCount = 4 // 1 -> 2, 2 -> 3, 3 -> 4, 4 -> 5 with catchup
    for (i <- 1 to stagesCount) yield {
      val stage = generateStage()
      if (i < stagesCount) {
        stage
      } else {
        // Let the last stage have some delay after onboarding to test both onboarding and catching up
        //  from at least 1 onboarded node.
        stage.copy(
          simulationSettings = stage.simulationSettings.copy(
            becomingOnlineAfterOnboardingDelay =
              SimulationSettings.DefaultBecomingOnlineAfterOnboardingDelay
          )
        )
      }
    }
  }

  private def generateStage() =
    SimulationTestStage(
      numberOfOnboardedPeers = numberOfRandomlyOnboardedPeers,
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
        peerOnboardingDelays =
          LazyList.iterate(newOnboardingDelay(), numberOfRandomlyOnboardedPeers)(_ =>
            newOnboardingDelay()
          ),
        // Delay of zero doesn't make the test rely on catch-up, as onboarded nodes will buffer all messages since
        //  the activation, and thus won't fall behind.
        becomingOnlineAfterOnboardingDelay = 0.seconds,
      ),
    )

  private def newOnboardingDelay(): FiniteDuration =
    generatePeerOnboardingDelay(
      durationOfFirstPhaseWithFaults,
      randomSourceToCreateSettings,
    )
}

class BftOrderingSimulationTestWithConcurrentOnboardingsNoFaults extends BftOrderingSimulationTest {

  override val numberOfRuns: Int = 1

  override val numberOfInitialPeers: Int = 1 // f = 0

  private val numberOfOnboardedPeers = 3 // n = 4, f = 1

  // Onboard all nodes at the same time in the middle of the first phase.
  // TODO(#23819) test onboarding with slightly different delays (e.g., 1 microsecond) still within a single epoch
  //  this can only be done when #23659 is implemented
  private val peerOnboardingDelay = 30.seconds
  private val durationOfFirstPhase = 1.minute
  private val durationOfSecondPhase = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateStages(): Seq[SimulationTestStage] = Seq(
    SimulationTestStage(
      numberOfOnboardedPeers = numberOfOnboardedPeers,
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhase,
        durationOfSecondPhase,
        peerOnboardingDelays =
          LazyList.continually(peerOnboardingDelay).take(numberOfOnboardedPeers),
        // Delay of zero doesn't make the test rely on catch-up, as onboarded nodes will buffer all messages since
        //  the activation, and thus won't fall behind.
        becomingOnlineAfterOnboardingDelay = 0.seconds,
      ),
    )
  )
}

class BftOrderingSimulationTest2NodesBootstrap extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 100
  override val numberOfInitialPeers: Int = 2

  private val durationOfFirstPhaseWithFaults = 2.seconds
  private val durationOfSecondPhaseWithoutFaults = 2.seconds

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateStages(): Seq[SimulationTestStage] = Seq(
    SimulationTestStage(
      numberOfOnboardedPeers = 0,
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
      ),
    )
  )
}

// Simulation test about empty blocks, needed to pass the liveness check.
class BftOrderingEmptyBlocksSimulationTest extends BftOrderingSimulationTest {
  // At the moment of writing, the test requires 12 runs to fail on the liveness check when there's no "silent network detection".
  override val numberOfRuns: Int = 15
  override val numberOfInitialPeers: Int = 2
  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random = new Random(4)

  override def generateStages(): Seq[SimulationTestStage] = Seq(
    SimulationTestStage(
      numberOfOnboardedPeers = 0,
      simulationSettings = SimulationSettings(
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
        // This value is lower than the default to prevent view changes from ensuring liveness (as we want empty blocks to ensure it).
        // When the simulation becomes "healthy", we don't know when the last crash (resetting the view change timeout)
        // or view change happened. Similarly, we don't know how "advanced" the empty block creation at that moment is.
        // Since the simulation is deterministic and runs multiple times, we can base this value on the empty block creation
        // interval to get the desired test coverage.
        livenessCheckInterval = AvailabilityModuleConfig.EmptyBlockCreationInterval * 2 + 1.second,
      ),
    )
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
