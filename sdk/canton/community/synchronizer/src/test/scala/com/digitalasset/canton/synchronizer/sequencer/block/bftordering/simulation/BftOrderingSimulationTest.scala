// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory, NamedLogging, TracedLogger}
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
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
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
  SimulationTestSettings,
  SimulationTestStageSettings,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.SequencerSnapshotOnboardingManager.BftOnboardingData
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
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pprint.PPrinter

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.util.Random

/** Simulation testing troubleshooting tips & tricks:
  *
  *   - When a test fails, it prints the configuration it failed with (including all the seeds that
  *     were necessary for the failure). The configuration can then be copy-pasted one-to-one into
  *     [[generateSettings]] of a new or existing test case. Then, the [[numberOfRuns]] can be
  *     lowered down to 1. This narrows the investigation scope and makes logs shorter. If you take
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
  def generateSettings: SimulationTestSettings

  def allowedWarnings: Seq[LogEntry => Assertion] = Seq.empty

  def warnLogAssertion(logEntry: LogEntry): Assertion =
    if (allowedWarnings.isEmpty) {
      fail(
        s"Test should not produce warning logs but got: ${logEntry.message}"
      )
    } else {
      exists(
        Table("assertions", allowedWarnings*)
      )(_(logEntry))
    }

  private val noopMetrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

  private def hostname(i: Int) = s"system$i"

  protected def makeEndpoint(i: Int): P2PEndpoint =
    PlainTextP2PEndpoint(hostname(i), Port.tryCreate(0)).asInstanceOf[P2PEndpoint]

  it should "run with no issues" in {

    for (runNumber <- 1 to numberOfRuns) {
      logger.info(s"Starting run $runNumber (of $numberOfRuns)")

      val simulationTestSettings = generateSettings
      val numberOfInitialNodes = simulationTestSettings.numberOfInitialNodes
      val epochLength = simulationTestSettings.epochLength

      val sendQueue = mutable.Queue.empty[(BftNodeId, Traced[BlockFormat.Block])]

      var firstNewlyOnboardedIndex = numberOfInitialNodes

      var simulationTestStage: Option[SimulationTestStage] = None

      val clock = new SimClock(SimulationStartTime, loggerFactory)

      val stages = simulationTestSettings.stages
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
              epochLength,
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
                   |
                   |  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
                   |    numberOfInitialNodes = $numberOfInitialNodes,
                   |    epochLength = EpochLength($epochLength),
                   |    stages = NonEmpty(
                   |      Seq,
                   |      ${stages
                    .map { stage =>
                      PPrinter(additionalHandlers = Simulation.fixupPrettyPrinting)(
                        stage,
                        initialOffset = 6,
                      )
                    }
                    .mkString(",\n")}
                   |    ),
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
      sendQueue: mutable.Queue[(BftNodeId, Traced[BlockFormat.Block])],
      epochLength: EpochLength,
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
    val config = BftBlockOrdererConfig(
      epochLength = epochLength,
      availabilityNumberOfAttemptsOfDownloadingOutputFetchBeforeWarning = 100,
    )

    SimulationInitializer(
      {
        case BftOnboardingData(
              initialApplicationHeight,
              sequencerSnapshotAdditionalInfo,
            ) =>
          // Forces always querying for an up-to-date topology, so that we simulate correctly topology changes.
          val requestInspector = new RequestInspector {
            override def isRequestToAllMembersOfSynchronizer(
                blockMetadata: BlockMetadata,
                requestNumber: Int,
                request: OrderingRequest,
                maxBytesToDecompress: MaxBytesToDecompress,
                logger: TracedLogger,
                traceContext: TraceContext,
            )(implicit synchronizerProtocolVersion: ProtocolVersion): Boolean = true
          }

          new BftOrderingModuleSystemInitializer[SimulationEnv, SimulationP2PNetworkManager[
            BftOrderingMessage
          ]](
            thisBftNodeId,
            config,
            initialApplicationHeight,
            EpochLength(config.epochLength),
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
