// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.protocol.MaxRequestSizeToDeserialize
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.memory.SimulationAvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.SimulationEpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.EpochChecker
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.RequestInspector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory.SimulationOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PNetworkOutModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.memory.SimulationP2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.memory.SimulationBftOrdererPruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.{
  BftBlockOrdererConfig,
  BftOrderingModuleSystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.SimulationBlockSubscription
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Mempool
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.{
  SimulationEnv,
  SimulationInitializer,
  SimulationP2PNetworkManager,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering.{
  BftOrderingVerifier,
  IssClient,
  SimulationTestStageSettings,
  TopologySettings,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.SequencerSnapshotOnboardingManager.BftOnboardingData
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.SimulationTopologyHelpers.generateNodeOnboardingDelay
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.{
  NodeSimulationTopologyData,
  NodeSimulationTopologyDataFactory,
  SequencerSnapshotOnboardingManager,
  SimulationOrderingTopologyProvider,
  SimulationTopologyHelpers,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  endpointToTestBftNodeId,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pprint.PPrinter

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
trait BftOrderingSimulationTest extends AnyFlatSpec with BftSequencerBaseTest {

  import BftOrderingSimulationTest.*

  def numberOfRuns: Int
  def numberOfInitialNodes: Int
  def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]]

  def warnLogAssertion(logEntry: LogEntry): Assertion = fail(
    s"Test should not produce warning logs but got: ${logEntry.message}"
  )

  private val noopMetrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

  private def hostname(i: Int) = s"system$i"

  protected def makeEndpoint(i: Int): P2PEndpoint =
    PlainTextP2PEndpoint(hostname(i), Port.tryCreate(0)).asInstanceOf[P2PEndpoint]

  it should "run with no issues" in {

    for (runNumber <- 1 to numberOfRuns) {

      logger.info(s"Starting run $runNumber (of $numberOfRuns)")

      val sendQueue = mutable.Queue.empty[(BftNodeId, BlockFormat.Block)]

      var firstNewlyOnboardedIndex = numberOfInitialNodes

      var simulationTestStage: Option[SimulationTestStage] = None

      val clock = new SimClock(SimulationStartTime, loggerFactory)

      val stages = generateStages()
      val stagesCount = stages.size

      val initialEndpoints =
        (0 until numberOfInitialNodes).map(makeEndpoint)

      val firstStage = stages.head1
      // the topologyRandom is shared in all stages, we only set the seed from the first stage.
      val topologyRandom = new Random(firstStage.topologySettings.randomSeed)
      var alreadyOnboardedAll =
        initialEndpoints.map { endpoint =>
          endpointToTestBftNodeId(endpoint) -> SimulationTestNodeData(
            simulationTopologyData = SimulationTopologyHelpers.generateSingleSimulationTopologyData(
              SimulationStartTime,
              NodeSimulationTopologyDataFactory.generate(
                topologyRandom,
                onboardingTime = None,
                shouldPerformOffboarding =
                  firstStage.topologySettings.nodesToOffboard.contains(endpoint),
                topologySettings = firstStage.topologySettings,
                stopKeyRotations = firstStage.simulationSettings.durationOfFirstPhaseWithFaults,
              ),
            ),
            endpoint,
            stores = {
              val simulationEpochStore = new SimulationEpochStore(firstStage.failOnViewChange)
              BftOrderingStores(
                new SimulationP2PEndpointsStore(
                  initialEndpoints.filterNot(_ == endpoint).toSet
                ),
                new SimulationAvailabilityStore(),
                simulationEpochStore,
                epochStoreReader = simulationEpochStore,
                new SimulationOutputMetadataStore(fail(_)),
                new SimulationBftOrdererPruningSchedulerStore(),
              )
            },
            initializeImmediately = true,
          )
        }.toMap

      logger.debug(s"$alreadyOnboardedAll")

      val epochChecker = new SimEpochChecker(loggerFactory)
      val allSimulationTestNodeDataCell =
        new AtomicReference[Map[BftNodeId, SimulationTestNodeData]](Map.empty)
      stages.zipWithIndex.foreach {
        case (SimulationTestStageSettings(simSettings, topologySettings, failOnViewChange), idx) =>
          val stageStart = clock.now

          logger.info(s"Starting stage ${idx + 1} (of $stagesCount) at $stageStart")

          val availabilityRandom = new Random(simSettings.localSettings.randomSeed)

          val numberOfOnboardedNodes = topologySettings.nodeOnboardingDelays.size
          val newlyOnboardedAll = topologySettings.nodeOnboardingDelays.zipWithIndex.view.map {
            case (delay, i) =>
              val endpoint = makeEndpoint(i + firstNewlyOnboardedIndex)
              val stores = {
                val simulationEpochStore = new SimulationEpochStore(failOnViewChange)
                BftOrderingStores(
                  // For newly onboarded nodes we will add endpoints to all other nodes when it gets added
                  // so the store will start empty
                  new SimulationP2PEndpointsStore(Set.empty),
                  new SimulationAvailabilityStore(),
                  simulationEpochStore,
                  epochStoreReader = simulationEpochStore,
                  new SimulationOutputMetadataStore(fail(_)),
                  new SimulationBftOrdererPruningSchedulerStore(),
                )
              }
              endpointToTestBftNodeId(endpoint) -> SimulationTestNodeData(
                simulationTopologyData =
                  SimulationTopologyHelpers.generateSingleSimulationTopologyData(
                    stageStart,
                    NodeSimulationTopologyDataFactory.generate(
                      topologyRandom,
                      onboardingTime = Some(delay),
                      shouldPerformOffboarding =
                        topologySettings.nodesToOffboard.contains(endpoint),
                      topologySettings = topologySettings,
                      stopKeyRotations = simSettings.durationOfFirstPhaseWithFaults,
                    ),
                  ),
                endpoint = endpoint,
                stores,
                initializeImmediately = false,
              )
          }.toMap
          logger.debug(s"$newlyOnboardedAll")

          val allNodes = alreadyOnboardedAll ++ newlyOnboardedAll
          allSimulationTestNodeDataCell.set(allNodes)
          def initializer(
              endpoint: P2PEndpoint,
              stores: BftOrderingStores[SimulationEnv],
              initializeImmediately: Boolean,
          ): SimulationInitializerT =
            newInitializer(
              endpoint,
              () =>
                allSimulationTestNodeDataCell.get().map { case (_, endpointData) =>
                  endpointData.endpoint -> endpointData.simulationTopologyData
                },
              stores,
              sendQueue,
              clock,
              availabilityRandom,
              epochChecker,
              simSettings,
              initializeImmediately,
            )
          simulationTestStage = simulationTestStage match {
            case None => // First stage
              val model =
                new BftOrderingVerifier(
                  sendQueue,
                  allNodes.view.mapValues(_.stores.outputStore).toMap,
                  allNodes.view.mapValues(_.simulationTopologyData.onboardingTime).toMap,
                  allNodes.view.flatMap { case (node, sim) =>
                    sim.simulationTopologyData.offboardingTime.map(node -> _)
                  }.toMap,
                  alreadyOnboardedAll.keys.toSeq,
                  simSettings,
                  DefaultEpochLength,
                  loggerFactory,
                )
              val onboardingManager = new SequencerSnapshotOnboardingManager(
                newlyOnboardedAll.view.mapValues(_.simulationTopologyData.onboardingTime).toMap,
                alreadyOnboardedAll.keys.toSeq,
                allNodes.view.mapValues(_.endpoint).toMap,
                allNodes.view.mapValues(_.stores).toMap,
                model,
                topologySettings,
                topologyRandom,
              )
              val simulation =
                SimulationModuleSystem(
                  allNodes.map { case (_, endpointData) =>
                    endpointData.endpoint -> initializer(
                      endpointData.endpoint,
                      endpointData.stores,
                      endpointData.initializeImmediately,
                    )
                  },
                  onboardingManager,
                  simSettings,
                  clock,
                  timeouts,
                  loggerFactory,
                )
              Some(SimulationTestStage(simulation, model, onboardingManager))

            case Some(stage) => // Subsequent stages
              val newModel =
                stage.model.newStage(
                  simSettings,
                  newlyOnboardedAll.view.mapValues(_.simulationTopologyData.onboardingTime).toMap,
                  newlyOnboardedAll.view.flatMap { case (node, sim) =>
                    sim.simulationTopologyData.offboardingTime.map(node -> _)
                  }.toMap,
                  newlyOnboardedAll.view.mapValues(_.stores.outputStore).toMap,
                )
              val newOnboardingManager = stage.onboardingManager.newStage(
                newlyOnboardedAll.view.mapValues(_.simulationTopologyData.onboardingTime).toMap,
                allNodes.view.mapValues(_.endpoint).toMap,
                newModel,
                topologySettings,
              )
              Some(
                SimulationTestStage(
                  stage.simulation.newStage(
                    simSettings,
                    newOnboardingManager,
                    newlyOnboardedAll.map { case (_, endpointData) =>
                      endpointData.endpoint ->
                        initializer(
                          endpointData.endpoint,
                          endpointData.stores,
                          endpointData.initializeImmediately,
                        )
                    },
                  ),
                  newModel,
                  newOnboardingManager,
                )
              )
          }

          val stage =
            simulationTestStage
              .getOrElse(fail("The simulation object was not set but it should always be"))

          try {
            loggerFactory.assertLoggedWarningsAndErrorsSeq(
              stage.simulation.run(stage.model),
              log => forAll(log)(warnLogAssertion),
            )
          } catch {
            case e: Throwable =>
              val testClass =
                s"""class <testName> extends BftOrderingSimulationTest {
                   |  override val numberOfRuns: Int = 1
                   |  override val numberOfInitialNodes = $numberOfInitialNodes
                   |
                   |  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = NonEmpty(
                   |    Seq,
                   |    ${stages
                    .map { stage =>
                      PPrinter(additionalHandlers = Simulation.fixupPrettyPrinting)(
                        stage,
                        initialOffset = 4,
                      )
                    }
                    .mkString(",\n")}
                   |  )
                   |}""".stripMargin

              logger.error(
                s"Uncaught exception during simulation, test class that will reproduce:\n$testClass",
                e,
              )
              fail(e)
          }

          // Prepare for next stage
          firstNewlyOnboardedIndex += numberOfOnboardedNodes

          alreadyOnboardedAll = allNodes
      }
    }
  }

  def newInitializer(
      endpoint: P2PEndpoint,
      getAllEndpointsToTopologyData: () => Map[P2PEndpoint, NodeSimulationTopologyData],
      stores: BftOrderingStores[SimulationEnv],
      sendQueue: mutable.Queue[(BftNodeId, BlockFormat.Block)],
      clock: Clock,
      availabilityRandom: Random,
      epochChecker: EpochChecker,
      simSettings: SimulationSettings,
      initializeImmediately: Boolean,
  ): SimulationInitializerT = {

    val logger = loggerFactory.append("endpoint", s"${endpoint.address}")

    val thisBftNodeId = endpointToTestBftNodeId(endpoint)
    val orderingTopologyProvider =
      new SimulationOrderingTopologyProvider(
        thisBftNodeId,
        getAllEndpointsToTopologyData,
        loggerFactory,
      )

    val p2pGrpcConnectionState = new P2PGrpcConnectionState(thisBftNodeId, logger)

    SimulationInitializer(
      {
        case BftOnboardingData(
              initialApplicationHeight,
              sequencerSnapshotAdditionalInfo,
            ) =>
          // Forces always querying for an up-to-date topology, so that we simulate correctly topology changes.
          val requestInspector = new RequestInspector {
            override def isRequestToAllMembersOfSynchronizer(
                request: OrderingRequest,
                maxRequestSizeToDeserialize: MaxRequestSizeToDeserialize,
                logger: TracedLogger,
                traceContext: TraceContext,
            )(implicit synchronizerProtocolVersion: ProtocolVersion): Boolean = true
          }

          new BftOrderingModuleSystemInitializer[SimulationEnv, SimulationP2PNetworkManager[
            BftOrderingMessage
          ]](
            thisBftNodeId,
            BftBlockOrdererConfig(
              availabilityNumberOfAttemptsOfDownloadingOutputFetchBeforeWarning = 100
            ),
            initialApplicationHeight,
            DefaultEpochLength,
            stores,
            orderingTopologyProvider,
            new SimulationBlockSubscription(thisBftNodeId, sendQueue),
            sequencerSnapshotAdditionalInfo,
            bootstrapMembership =>
              new P2PNetworkOutModule.State(p2pGrpcConnectionState, bootstrapMembership),
            clock,
            availabilityRandom,
            noopMetrics,
            logger,
            timeouts,
            requestInspector,
            epochChecker,
          )
      },
      IssClient.initializer(simSettings, thisBftNodeId, logger, timeouts),
      p2pGrpcConnectionState,
      initializeImmediately,
    )
  }
}

object BftOrderingSimulationTest {

  private class SimEpochChecker(override val loggerFactory: NamedLoggerFactory)
      extends EpochChecker
      with Matchers
      with NamedLogging {

    private val allPrevious = mutable.Map.empty[EpochNumber, (BftNodeId, Membership)]

    override def check(
        thisNode: BftNodeId,
        epochNumber: EpochNumber,
        membership: Membership,
    ): Unit =
      allPrevious.get(epochNumber) match {
        case Some((otherNode, otherMembership)) =>
          withClue(s"first node: $otherNode, second node: $thisNode") {
            membership.leaders shouldBe otherMembership.leaders
          }
        case None =>
          allPrevious(epochNumber) = (thisNode, membership)
      }

  }

  val SimulationStartTime: CantonTimestamp = CantonTimestamp.Epoch

  private type ApplyBft[F[_, _, _, _]] = F[
    BftOnboardingData,
    BftOrderingMessage,
    Mempool.Message,
    Unit,
  ]

  private type SimulationInitializerT = ApplyBft[SimulationInitializer]
  private type SimulationT = ApplyBft[Simulation]

  final case class SimulationTestNodeData(
      simulationTopologyData: NodeSimulationTopologyData,
      endpoint: P2PEndpoint,
      stores: BftOrderingStores[SimulationEnv],
      initializeImmediately: Boolean,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[SimulationTestNodeData.this.type] = adHocPrettyInstance
  }

  final case class SimulationTestStage(
      simulation: SimulationT,
      model: BftOrderingVerifier,
      onboardingManager: SequencerSnapshotOnboardingManager,
  )
}

class BftOrderingSimulationTest1NodeNoFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 10
  override val numberOfInitialNodes = 1

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = NonEmpty(
    Seq,
    SimulationTestStageSettings(
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
      TopologySettings(randomSourceToCreateSettings.nextLong()),
    ),
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

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = {
    val stagesCount = 4 // 1 -> 2, 2 -> 3, 3 -> 4, 4 -> 5 with delay
    val extraStages = 2 to stagesCount

    for (i <- NonEmpty(Seq, 1, extraStages*)) yield {
      val stage = generateStage()
      if (i < stagesCount) {
        stage
      } else {
        // Let the last stage have some delay after onboarding to test onboarding with more epochs to transfer,
        //  i.e, higher end epoch calculated.
        stage.copy(
          topologySettings = stage.topologySettings.copy(
            becomingOnlineAfterOnboardingDelay =
              TopologySettings.DefaultBecomingOnlineAfterOnboardingDelay
          )
        )
      }
    }
  }

  private def generateStage() =
    SimulationTestStageSettings(
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
      TopologySettings(
        randomSeed = randomSourceToCreateSettings.nextLong(),
        nodeOnboardingDelays = List(newOnboardingDelay()),
        // Delay of zero doesn't make the test rely on catch-up, as onboarded nodes will buffer all messages since
        //  the activation, and thus won't fall behind.
        becomingOnlineAfterOnboardingDelay = 0.seconds,
      ),
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
    new Random(3) // Manually remove the seed for fully randomized local runs.

  // Onboard all nodes around the same time in the middle of the first phase.
  private val baseOnboardingDelay = 30.seconds
  private val durationOfFirstPhase = 1.minute
  private val durationOfSecondPhase = 1.minute

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = NonEmpty(
    Seq,
    SimulationTestStageSettings(
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhase,
        durationOfSecondPhase,
      ),
      TopologySettings(
        randomSeed = randomSourceToCreateSettings.nextLong(),
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
      ),
    ),
  )
}

class BftOrderingSimulationTestWithOnboardingAndKeyRotationsNoFaults
    extends BftOrderingSimulationTest {
  override def numberOfRuns: Int = 5
  override def numberOfInitialNodes: Int = 2

  private val random = new Random(4)
  private val durationOfFirstPhaseWithFaults = 1.minute

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] =
    NonEmpty(
      Seq,
      SimulationTestStageSettings(
        SimulationSettings(
          LocalSettings(random.nextLong()),
          NetworkSettings(random.nextLong()),
          durationOfFirstPhaseWithFaults,
        ),
        TopologySettings(
          random.nextLong(),
          shouldDoKeyRotations = true,
          nodeOnboardingDelays =
            Seq(generateNodeOnboardingDelay(durationOfFirstPhaseWithFaults, random)),
        ),
      ),
    )
}

// Allows catch-up state transfer testing without requiring CFT.
class BftOrderingSimulationTestWithPartitions extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 4
  override val numberOfInitialNodes = 4

  private val durationOfFirstPhaseWithPartitions = 2.minutes

  // Manually remove the seed for fully randomized local runs.
  private val randomSourceToCreateSettings: Random = new Random(4)

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = NonEmpty(
    Seq,
    SimulationTestStageSettings(
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
      ),
      TopologySettings(randomSourceToCreateSettings.nextLong()),
    ),
  )
}

class BftOrderingSimulationTest2NodesBootstrap extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 100
  override val numberOfInitialNodes: Int = 2

  private val durationOfFirstPhaseWithFaults = 2.seconds
  private val durationOfSecondPhaseWithoutFaults = 2.seconds

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = NonEmpty(
    Seq,
    SimulationTestStageSettings(
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
      TopologySettings(randomSourceToCreateSettings.nextLong()),
    ),
  )
}

// Simulation test about empty blocks, needed to pass the liveness check.
class BftOrderingEmptyBlocksSimulationTest extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 15
  override val numberOfInitialNodes: Int = 2

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random = new Random(4)

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = NonEmpty(
    Seq,
    SimulationTestStageSettings(
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
      ),
      TopologySettings(randomSourceToCreateSettings.nextLong()),
      // The purpose of this test is to make sure we progress time by making empty blocks. As such we don't want view
      // changes to happen (since they would also make the network do progress). So we need to explicitly check that
      // no ViewChanges happened.
      failOnViewChange = true,
    ),
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

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = NonEmpty(
    Seq,
    SimulationTestStageSettings(
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
      ),
      TopologySettings(randomSourceToCreateSettings.nextLong()),
    ),
  )
}

class BftOrderingSimulationTest2NodesCrashFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 10
  override val numberOfInitialNodes: Int = 2

  private val durationOfFirstPhaseWithFaults = 2.minutes
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // remove seed to randomly explore seeds

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = NonEmpty(
    Seq,
    SimulationTestStageSettings(
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong(),
          crashRestartChance = Probability(0.02),
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
      ),
      TopologySettings(randomSourceToCreateSettings.nextLong()),
    ),
  )
}

class BftOrderingSimulationTest4NodesCrashFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 5
  override val numberOfInitialNodes: Int = 4

  private val durationOfFirstPhaseWithFaults = 2.minutes
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // remove seed to randomly explore seeds

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = NonEmpty(
    Seq,
    SimulationTestStageSettings(
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong(),
          crashRestartChance = Probability(0.01),
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong()
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
      ),
      TopologySettings(randomSourceToCreateSettings.nextLong()),
    ),
  )
}

class BftOrderingSimulationTestOffboarding extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 4
  override val numberOfInitialNodes = 5

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(2) // Manually remove the seed for fully randomized local runs.

  override def warnLogAssertion(logEntry: LogEntry): Assertion = {
    // We might get messages from off boarded nodes, don't count these as errors.
    logEntry.message should include(
      "but it cannot be verified in the currently known dissemination topology"
    )
    logEntry.loggerName should include("AvailabilityModule")
  }

  override def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = NonEmpty(
    Seq,
    SimulationTestStageSettings(
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong(),
          crashRestartChance = Probability(0.02),
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong(),
          packetLoss = Probability(0.2),
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
      ),
      TopologySettings(
        randomSeed = randomSourceToCreateSettings.nextLong(),
        nodesToOffboard = Seq(makeEndpoint(2), makeEndpoint(4)),
      ),
    ),
  )
}
