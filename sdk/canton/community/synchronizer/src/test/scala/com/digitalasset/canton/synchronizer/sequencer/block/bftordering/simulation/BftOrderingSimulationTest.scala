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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.{
  NodeActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Mempool
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.{
  SimulationEnv,
  SimulationInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.BftOrderingSimulationTest.SimulationTestStage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering.{
  BftOrderingVerifier,
  IssClient,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.SimulationTopologyHelpers.{
  generateNodeOnboardingDelay,
  onboardingTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.{
  NodeActiveAtProvider,
  SimulationOrderingTopologyProvider,
  SimulationTopologyData,
  SimulationTopologyHelpers,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveRequest
import com.digitalasset.canton.time.{Clock, SimClock}
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

  import BftOrderingSimulationTest.*

  def numberOfRuns: Int
  def numberOfInitialNodes: Int
  def generateStages(): Seq[SimulationTestStage]

  private type SimulationT = Simulation[Option[
    NodeActiveAt
  ], BftOrderingServiceReceiveRequest, Mempool.Message, Unit]

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty
  private val noopMetrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

  private val initialApplicationHeight =
    BlockNumber.First // TODO(#17281): Change for restarts/crashes

  private lazy val initialIndexRange = 0 until numberOfInitialNodes
  private lazy val initialOnboardingTimes =
    initialIndexRange.map(_ => Genesis.GenesisTopologyActivationTime)
  private lazy val initialEndpoints =
    initialIndexRange.map(i => PlainTextP2PEndpoint(hostname(i), Port.tryCreate(0)))
  private lazy val initialEndpointsWithOnboardingTimes =
    initialEndpoints.zip(initialOnboardingTimes)
  private lazy val initialEndpointsToTopologyData =
    SimulationTopologyHelpers.generateSimulationTopologyData(
      initialEndpointsWithOnboardingTimes.toMap,
      loggerFactory,
    )
  private lazy val initialNodesToOnboardingTimes =
    initialEndpoints
      .zip(initialOnboardingTimes)
      .view
      .map { case (endpoint, onboardingTime) =>
        Simulation.endpointToNode(endpoint) -> onboardingTime
      }
      .toMap

  private def hostname(i: Int) = s"system$i"

  it should "run with no issues" in {

    for (runNumber <- 1 to numberOfRuns) {

      logger.info(s"Starting run $runNumber (of $numberOfRuns)")

      val initialEndpointsWithStores = initialEndpoints.map { endpoint =>
        val simulationEpochStore = new SimulationEpochStore()
        val stores = BftOrderingStores(
          new SimulationP2PEndpointsStore(initialEndpoints.filterNot(_ == endpoint).toSet),
          new SimulationAvailabilityStore(),
          simulationEpochStore,
          epochStoreReader = simulationEpochStore,
          new SimulationOutputMetadataStore(fail(_)),
        )
        endpoint -> stores
      }.toMap
      val initialNodesToStores =
        initialEndpointsWithStores.view.map { case (endpoint, store) =>
          Simulation.endpointToNode(endpoint) -> store
        }.toMap

      val sendQueue = mutable.Queue.empty[(BftNodeId, BlockFormat.Block)]

      var firstNewlyOnboardedIndex = initialEndpoints.size

      var alreadyOnboardedEndpoints = initialEndpoints
      var alreadyOnboardedEndpointsToTopologyData = initialEndpointsToTopologyData
      var alreadyOnboardedNodesToOnboardingTimes = initialNodesToOnboardingTimes
      var alreadyOnboardedNodesToStores = initialNodesToStores
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

        val numberOfOnboardedNodes = simSettings.nodeOnboardingDelays.size
        val newlyOnboardedEndpoints =
          (firstNewlyOnboardedIndex until firstNewlyOnboardedIndex + numberOfOnboardedNodes)
            .map(i => PlainTextP2PEndpoint(hostname(i), Port.tryCreate(0)))
        val newlyOnboardedEndpointsWithStores = newlyOnboardedEndpoints.map { endpoint =>
          val simulationEpochStore = new SimulationEpochStore()
          val stores = BftOrderingStores(
            // Creates a one-way connection from each new node to already onboarded endpoints
            new SimulationP2PEndpointsStore(alreadyOnboardedEndpoints.toSet),
            new SimulationAvailabilityStore(),
            simulationEpochStore,
            epochStoreReader = simulationEpochStore,
            new SimulationOutputMetadataStore(fail(_)),
          )
          endpoint -> stores
        }
        val newlyOnboardedEndpointsWithOnboardingTimes =
          newlyOnboardedEndpoints.zip(
            simSettings.nodeOnboardingDelays.map(onboardingTime(stageStart, _))
          )

        val newlyOnboardedEndpointsToTopologyData =
          SimulationTopologyHelpers.generateSimulationTopologyData(
            newlyOnboardedEndpointsWithOnboardingTimes.toMap,
            loggerFactory,
          )
        val allEndpointsToTopologyData =
          alreadyOnboardedEndpointsToTopologyData ++ newlyOnboardedEndpointsToTopologyData

        allEndpointsToTopologyDataCell.set(allEndpointsToTopologyData)

        def initializer(
            endpoint: P2PEndpoint,
            stores: BftOrderingStores[SimulationEnv],
            initializeImmediately: Boolean,
        ) =
          newInitializer(
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
          newlyOnboardedEndpointsWithStores.map { case (endpoint, stores) =>
            endpoint -> initializer(
              endpoint,
              stores,
              initializeImmediately = false,
            )
          }.toMap
        val newlyOnboardedNodesToOnboardingTimes =
          newlyOnboardedEndpointsWithOnboardingTimes.view.map { case (endpoint, onboardingTime) =>
            Simulation.endpointToNode(endpoint) -> onboardingTime
          }.toMap
        val newlyOnboardedNodesToStores =
          newlyOnboardedEndpointsWithStores.view.map { case (endpoint, store) =>
            Simulation.endpointToNode(endpoint) -> store
          }.toMap

        val allNodesToOnboardingTimes =
          alreadyOnboardedNodesToOnboardingTimes ++ newlyOnboardedNodesToOnboardingTimes
        val allNodesToStores =
          alreadyOnboardedNodesToStores ++ newlyOnboardedNodesToStores

        simulationAndModel = simulationAndModel match {
          case None => // First stage
            val initialTopologyInitializers =
              initialEndpointsWithStores.map { case (endpoint, stores) =>
                endpoint -> initializer(
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
                new NodeActiveAtProvider(
                  allNodesToOnboardingTimes,
                  allNodesToStores,
                ),
                simSettings,
                clock,
                timeouts,
                loggerFactory,
              )
            val model =
              new BftOrderingVerifier(
                sendQueue,
                allNodesToStores.view.mapValues(stores => stores.outputStore).toMap,
                allNodesToOnboardingTimes,
                simSettings,
                loggerFactory,
              )
            Some(simulation -> model)

          case Some((previousSimulation, previousModel)) => // Subsequent stages
            Some(
              previousSimulation.newStage(
                simSettings,
                new NodeActiveAtProvider(
                  allNodesToOnboardingTimes,
                  allNodesToStores,
                ),
                newlyOnboardedTopologyInitializers,
              ) ->
                previousModel.newStage(
                  simSettings,
                  newlyOnboardedNodesToOnboardingTimes,
                  newlyOnboardedNodesToStores.view.mapValues(_.outputStore).toMap,
                )
            )
        }

        val (simulation, model) =
          simulationAndModel
            .getOrElse(fail("The simulation object was not set but it should always be"))

        simulation.run(model)

        // Prepare for next stage
        firstNewlyOnboardedIndex += numberOfOnboardedNodes
        alreadyOnboardedEndpoints ++= newlyOnboardedEndpoints
        alreadyOnboardedEndpointsToTopologyData = allEndpointsToTopologyData
        alreadyOnboardedNodesToOnboardingTimes = allNodesToOnboardingTimes
        alreadyOnboardedNodesToStores = allNodesToStores
      }
    }
  }

  def newInitializer(
      endpoint: P2PEndpoint,
      getAllEndpointsToTopologyData: () => Map[P2PEndpoint, SimulationTopologyData],
      stores: BftOrderingStores[SimulationEnv],
      sendQueue: mutable.Queue[(BftNodeId, BlockFormat.Block)],
      clock: Clock,
      availabilityRandom: Random,
      simSettings: SimulationSettings,
      initializeImmediately: Boolean,
  ): SimulationInitializer[Option[
    NodeActiveAt
  ], BftOrderingServiceReceiveRequest, Mempool.Message, Unit] = {

    val logger = loggerFactory.append("endpoint", s"$endpoint")

    val thisNode = Simulation.endpointToNode(endpoint)
    val orderingTopologyProvider =
      new SimulationOrderingTopologyProvider(
        thisNode,
        getAllEndpointsToTopologyData,
        loggerFactory,
      )

    val (genesisTopology, _) =
      SimulationTopologyHelpers.resolveOrderingTopology(
        orderingTopologyProvider.getOrderingTopologyAt(TopologyActivationTime(SimulationStartTime))
      )

    SimulationInitializer(
      (maybeActiveAt: Option[NodeActiveAt]) => {
        val sequencerSnapshotAdditionalInfo =
          if (genesisTopology.contains(thisNode))
            None
          else {
            // Non-simulated snapshots contain information about other nodes as well.
            //  We skip it here for simplicity. Fully reflecting the non-simulated logic would be pointless,
            //  as it would still be totally different code.
            maybeActiveAt.map(activeAt =>
              SequencerSnapshotAdditionalInfo(Map(thisNode -> activeAt))
            )
          }

        // Forces always querying for an up-to-date topology, so that we simulate correctly topology changes.
        val requestInspector: RequestInspector =
          (_: OrderingRequest, _: ProtocolVersion, _: TracedLogger, _: TraceContext) => true

        new BftOrderingModuleSystemInitializer[SimulationEnv](
          testedProtocolVersion,
          thisNode,
          BftBlockOrdererConfig(),
          initialApplicationHeight,
          IssConsensusModule.DefaultEpochLength,
          stores,
          orderingTopologyProvider,
          new SimulationBlockSubscription(thisNode, sendQueue),
          sequencerSnapshotAdditionalInfo,
          clock,
          availabilityRandom,
          noopMetrics,
          logger,
          timeouts,
          requestInspector,
        )
      },
      IssClient.initializer(simSettings, thisNode, logger, timeouts),
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
  override val numberOfInitialNodes: Int = 1

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

  override val numberOfInitialNodes: Int = 1

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
        nodeOnboardingDelays = List(newOnboardingDelay()),
        // Delay of zero doesn't make the test rely on catch-up, as onboarded nodes will buffer all messages since
        //  the activation, and thus won't fall behind.
        becomingOnlineAfterOnboardingDelay = 0.seconds,
      )
    )

  private def newOnboardingDelay(): FiniteDuration =
    generateNodeOnboardingDelay(
      durationOfFirstPhaseWithFaults,
      randomSourceToCreateSettings,
    )
}

class BftOrderingSimulationTestWithConcurrentOnboardingsNoFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 3
  override val numberOfInitialNodes: Int = 1 // f = 0
  private val numberOfOnboardedNodes = 6 // n = 7, f = 2

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  // Onboard all nodes around the same time in the middle of the first phase.
  private val baseOnboardingDelay = 30.seconds
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
        nodeOnboardingDelays = LazyList
          .continually {
            val onboardingTimeDriftProbability = Probability(0.3)
            // The idea is to test scenarios where onboarding times are both the same and slightly different.
            //  Hopefully, the onboarding times land in the same epoch. It can be ensured with higher probability
            //  by increasing the numbers of runs and nodes.
            if (onboardingTimeDriftProbability.flipCoin(randomSourceToCreateSettings))
              baseOnboardingDelay.plus(1.microsecond)
            else baseOnboardingDelay
          }
          .take(numberOfOnboardedNodes),
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
  override val numberOfInitialNodes: Int = 4

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
  override val numberOfInitialNodes: Int = 2

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
  override val numberOfInitialNodes: Int = 2
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
  override val numberOfInitialNodes: Int = 2

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
          Some(PositiveInt.tryCreate(BftBlockOrdererConfig.DefaultMaxRequestPayloadBytes - 100)),
      )
    )
  )
}

/*
// TODO(#17284) Activate when we can handle the crash restart fault
class BftOrderingSimulationTest2NodesCrashFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 10
  override val numberOfNodes: Int = 2

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
