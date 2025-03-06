// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.TracedLogger
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.network.data.memory.SimulationP2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.RequestInspector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory.SimulationOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.SimulationBlockSubscription
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.{
  PeerActiveAt,
  SequencerSnapshotAdditionalInfo,
}
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
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveRequest
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
  *   - When a test fails, it prints the configuration it failed with (including all the seeds that
  *     were necessary for the failure). The configuration can then be copy-pasted one-to-one into
  *     [[generateStages]] of a new or existing test case. Then, the [[numberOfRuns]] can be lowered
  *     down to 1. This narrows the investigation scope and makes logs shorter. If you take
  *     advantage of the logs, remember to remove the log file before investigating such a run. It
  *     can be automated using the command line or in IntelliJ by specifying a "Run external tool"
  *     under "Before launch" in the Run Configurations with "rm" as a program and
  *     "log/canton_test.log" as arguments.
  *
  *   - Since simulation test runs are fully deterministic, i.e., messages are always processed in
  *     the same order, debugging is significantly easier, e.g., you can use conditional breakpoints
  *     with a potentially problematic block number.
  *
  *   - Because simulation tests control time and thus are extremely fast, they can be used for
  *     profiling and provide performance information.
  *
  *   - It's sometimes useful to set a breakpoint somewhere in the [[Simulation]] class to be able
  *     to inspect the [[Simulation.currentHistory]]. It should give you an idea of what was
  *     happening during the test.
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
    initialPeersIndexRange.map(i => PlainTextP2PEndpoint(peerHostname(i), Port.tryCreate(0)))
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

      val initialPeersWithStores = initialPeerEndpoints.map { endpoint =>
        val simulationEpochStore = new SimulationEpochStore()
        val stores = BftOrderingStores(
          new SimulationP2PEndpointsStore(initialPeerEndpoints.filterNot(_ == endpoint).toSet),
          new SimulationAvailabilityStore(),
          simulationEpochStore,
          epochStoreReader = simulationEpochStore,
          new SimulationOutputMetadataStore(fail(_)),
        )
        endpoint -> stores
      }.toMap
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
        new AtomicReference[Map[P2PEndpoint, SimulationTopologyData]](Map.empty)
      def getAllEndpointsToTopologyData = allEndpointsToTopologyDataCell.get()

      val stages = generateStages()
      val stagesCount = stages.size
      stages.zipWithIndex.foreach { case (SimulationTestStage(simSettings), idx) =>
        val stageStart = clock.now

        logger.info(s"Starting stage ${idx + 1} (of $stagesCount) at $stageStart")

        val availabilityRandom = new Random(simSettings.localSettings.randomSeed)

        val numberOfOnboardedPeers = simSettings.peerOnboardingDelays.size
        val newlyOnboardedPeerEndpoints =
          (firstNewlyOnboardedPeerIndex until firstNewlyOnboardedPeerIndex + numberOfOnboardedPeers)
            .map(i => PlainTextP2PEndpoint(peerHostname(i), Port.tryCreate(0)))
        val newlyOnboardedPeersWithStores = newlyOnboardedPeerEndpoints.map { endpoint =>
          val simulationEpochStore = new SimulationEpochStore()
          val stores = BftOrderingStores(
            new SimulationP2PEndpointsStore(alreadyOnboardedPeerEndpoints.toSet),
            new SimulationAvailabilityStore(),
            simulationEpochStore,
            epochStoreReader = simulationEpochStore,
            new SimulationOutputMetadataStore(fail(_)),
          )
          endpoint -> stores
        }
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
            endpoint: P2PEndpoint,
            stores: BftOrderingStores[SimulationEnv],
            initializeImmediately: Boolean,
        ) =
          newPeerInitializer(
            endpoint,
            () => getAllEndpointsToTopologyData,
            stores,
            sendQueue,
            clock,
            availabilityRandom,
            simSettings,
            initializeImmediately,
          )

        val newlyOnboardedTopologyInitializers =
          newlyOnboardedPeersWithStores.map { case (endpoint, stores) =>
            endpoint -> peerInitializer(
              endpoint,
              stores,
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
              initialPeersWithStores.map { case (endpoint, stores) =>
                endpoint -> peerInitializer(
                  endpoint,
                  stores,
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
                allSequencerIdsToStores.view.mapValues(stores => stores.outputStore).toMap,
                allSequencerIdsToOnboardingTimes,
                simSettings,
                loggerFactory,
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
                  newlyOnboardedSequencerIdsToStores.view.mapValues(_.outputStore).toMap,
                )
            )
        }

        val (simulation, model) =
          simulationAndModel
            .getOrElse(fail("The simulation object was not set but it should always be"))

        simulation.run(model)

        // Prepare for next stage
        firstNewlyOnboardedPeerIndex += numberOfOnboardedPeers
        alreadyOnboardedPeerEndpoints ++= newlyOnboardedPeerEndpoints
        alreadyOnboardedPeerEndpointsToTopologyData = allEndpointsToTopologyData
        alreadyOnboardedSequencerIdsToOnboardingTimes = allSequencerIdsToOnboardingTimes
        alreadyOnboardedSequencerIdsToStores = allSequencerIdsToStores
      }
    }
  }

  def newPeerInitializer(
      endpoint: P2PEndpoint,
      getAllEndpointsToTopologyData: () => Map[P2PEndpoint, SimulationTopologyData],
      stores: BftOrderingStores[SimulationEnv],
      sendQueue: mutable.Queue[(SequencerId, BlockFormat.Block)],
      clock: Clock,
      availabilityRandom: Random,
      simSettings: SimulationSettings,
      initializeImmediately: Boolean,
  ): SimulationInitializer[Option[
    PeerActiveAt
  ], BftOrderingServiceReceiveRequest, Mempool.Message, Unit] = {

    val peerLogger = loggerFactory.append("peer", s"$endpoint")

    val thisPeer = SimulationP2PNetworkManager.fakeSequencerId(endpoint)
    val orderingTopologyProvider =
      new SimulationOrderingTopologyProvider(
        thisPeer,
        getAllEndpointsToTopologyData,
        loggerFactory,
      )

    val (genesisTopology, _) =
      SimulationTopologyHelpers.resolveOrderingTopology(
        orderingTopologyProvider.getOrderingTopologyAt(TopologyActivationTime(SimulationStartTime))
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

        // Forces always querying for an up-to-date topology, so that we simulate correctly topology changes.
        val requestInspector: RequestInspector =
          (_: OrderingRequest, _: ProtocolVersion, _: TracedLogger, _: TraceContext) => true

        new BftOrderingModuleSystemInitializer[SimulationEnv](
          testedProtocolVersion,
          thisPeer,
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
      IssClient.initializer(simSettings, thisPeer, peerLogger, timeouts),
      initializeImmediately,
    )
  }
}

object BftOrderingSimulationTest {

  val SimulationStartTime: CantonTimestamp = CantonTimestamp.Epoch

  final case class SimulationTestStage(simulationSettings: SimulationSettings)
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
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
      )
    )
  )
}

class BftOrderingSimulationTestWithProgressiveOnboardingAndDelayNoFaults
    extends BftOrderingSimulationTest {

  override val numberOfRuns: Int = 2

  override val numberOfInitialPeers: Int = 1

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateStages(): Seq[SimulationTestStage] = {
    val stagesCount = 4 // 1 -> 2, 2 -> 3, 3 -> 4, 4 -> 5 with delay
    for (i <- 1 to stagesCount) yield {
      val stage = generateStage()
      if (i < stagesCount) {
        stage
      } else {
        // Let the last stage have some delay after onboarding to test onboarding with more epochs to transfer,
        //  i.e, higher end epoch calculated.
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
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
        peerOnboardingDelays = List(newOnboardingDelay()),
        // Delay of zero doesn't make the test rely on catch-up, as onboarded nodes will buffer all messages since
        //  the activation, and thus won't fall behind.
        becomingOnlineAfterOnboardingDelay = 0.seconds,
      )
    )

  private def newOnboardingDelay(): FiniteDuration =
    generatePeerOnboardingDelay(
      durationOfFirstPhaseWithFaults,
      randomSourceToCreateSettings,
    )
}

class BftOrderingSimulationTestWithConcurrentOnboardingsNoFaults extends BftOrderingSimulationTest {

  override val numberOfRuns: Int = 3

  override val numberOfInitialPeers: Int = 1 // f = 0

  private val numberOfOnboardedPeers = 3 // n = 4, f = 1

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  // Onboard all nodes around the same time in the middle of the first phase.
  private val basePeerOnboardingDelay = 30.seconds
  private val durationOfFirstPhase = 1.minute
  private val durationOfSecondPhase = 1.minute

  override def generateStages(): Seq[SimulationTestStage] = Seq(
    SimulationTestStage(
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhase,
        durationOfSecondPhase,
        peerOnboardingDelays = LazyList
          .continually {
            val onboardingTimeDriftProbability = Probability(0.3)
            // The idea is to test scenarios where onboarding times are both the same and slightly different.
            //  Hopefully, the onboarding times land in the same epoch. It can be ensured with higher probability
            //  by increasing the numbers of runs and nodes.
            if (onboardingTimeDriftProbability.flipCoin(randomSourceToCreateSettings))
              basePeerOnboardingDelay.plus(1.microsecond)
            else basePeerOnboardingDelay
          }
          .take(numberOfOnboardedPeers),
        // Delay of zero doesn't make the test rely on catch-up, as onboarded nodes will buffer all messages since
        //  the activation, and thus won't fall behind.
        becomingOnlineAfterOnboardingDelay = 0.seconds,
      )
    )
  )
}

// Allows catch-up state transfer testing without requiring CFT.
class BftOrderingSimulationTestWithPartitions extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 4
  override val numberOfInitialPeers: Int = 4

  private val durationOfFirstPhaseWithPartitions = 2.minutes

  // Manually remove the seed for fully randomized local runs.
  private val randomSourceToCreateSettings: Random = new Random(4)

  override def generateStages(): Seq[SimulationTestStage] = Seq {
    SimulationTestStage(
      SimulationSettings(
        LocalSettings(randomSourceToCreateSettings.nextLong()),
        NetworkSettings(
          randomSourceToCreateSettings.nextLong(),
          partitionStability = 20.seconds,
          unPartitionStability = 10.seconds,
          partitionProbability = Probability(0.1),
          partitionMode = PartitionMode.IsolateSingle,
          partitionSymmetry = PartitionSymmetry.Symmetric,
        ),
        durationOfFirstPhaseWithPartitions,
      )
    )
  }
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
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
      )
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
        clientRequestApproximateByteSize = None,
        // This value is lower than the default to prevent view changes from ensuring liveness (as we want empty blocks to ensure it).
        // When the simulation becomes "healthy", we don't know when the last crash (resetting the view change timeout)
        // or view change happened. Similarly, we don't know how "advanced" the empty block creation at that moment is.
        // Since the simulation is deterministic and runs multiple times, we can base this value on the empty block creation
        // interval to get the desired test coverage.
        livenessCheckInterval = AvailabilityModuleConfig.EmptyBlockCreationInterval * 2 + 1.second
          + 1.second, // TODO(#24283)  This value can't be too low, so adding an extra second
      )
    )
  )
}

// Note that simulation tests don't use a real network, so this test doesn't cover gRPC messages.
class BftOrderingSimulationTest2NodesLargeRequests extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 1
  override val numberOfInitialPeers: Int = 2

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateStages(): Seq[SimulationTestStage] = Seq(
    SimulationTestStage(
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
        // The test is a bit slow with the default interval
        clientRequestInterval = Some(10.seconds),
        clientRequestApproximateByteSize =
          // -100 to account for tags and payloads' prefixes
          // Exceeding the default size results in warning logs and dropping messages in Mempool
          Some(PositiveInt.tryCreate(BftBlockOrderer.DefaultMaxRequestPayloadBytes - 100)),
      )
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
