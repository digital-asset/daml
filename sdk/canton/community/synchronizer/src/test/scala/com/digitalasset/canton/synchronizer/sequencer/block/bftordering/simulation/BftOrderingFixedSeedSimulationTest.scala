// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.{
  ClientSettings,
  LocalSettings,
  NetworkSettings,
  PartitionMode,
  PartitionSymmetry,
  Probability,
  SimulationSettings,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering.{
  SimulationTestSettings,
  SimulationTestStageSettings,
  TopologySettings,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.SimulationTopologyHelpers.generateNodeOnboardingDelay
import org.scalatest.Assertion

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

class BftOrderingSimulationTest1NodeNoFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 10

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 1,
    stages = NonEmpty(
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
    ),
  )
}

class BftOrderingSimulationTestWithProgressiveOnboardingAndDelayNoFaults
    extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 2

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  private def generateStages(): NonEmpty[Seq[SimulationTestStageSettings]] = {
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

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 1,
    stages = generateStages(),
  )
}

class BftOrderingSimulationTestWithConcurrentOnboardingsNoFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 3

  private val numberOfOnboardedNodes = 6 // n = 7, f = 2

  private val randomSourceToCreateSettings: Random =
    new Random(3) // Manually remove the seed for fully randomized local runs.

  // Onboard all nodes around the same time in the middle of the first phase.
  private val baseOnboardingDelay = 30.seconds
  private val durationOfFirstPhase = 1.minute
  private val durationOfSecondPhase = 1.minute

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 1, // f = 0
    stages = NonEmpty(
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
    ),
  )
}

class BftOrderingSimulationTestWithOnboardingAndKeyRotationsNoFaults
    extends BftOrderingSimulationTest {
  override def numberOfRuns: Int = 5

  private val random = new Random(4)
  private val durationOfFirstPhaseWithFaults = 1.minute

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 2,
    stages = NonEmpty(
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
    ),
  )
}

// Allows catch-up state transfer testing without requiring CFT.
class BftOrderingSimulationTestWithPartitions extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 4

  private val durationOfFirstPhaseWithPartitions = 2.minutes

  // Manually remove the seed for fully randomized local runs.
  private val randomSourceToCreateSettings: Random = new Random(4)

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 4,
    stages = NonEmpty(
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
    ),
  )
}

class BftOrderingSimulationTest2NodesBootstrap extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 100

  private val durationOfFirstPhaseWithFaults = 2.seconds
  private val durationOfSecondPhaseWithoutFaults = 2.seconds

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 2,
    stages = NonEmpty(
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
    ),
  )
}

// Simulation test about empty blocks, needed to pass the liveness check.
class BftOrderingEmptyBlocksSimulationTest extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 15

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random = new Random(4)

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 2,
    stages = NonEmpty(
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
          clientSettings = ClientSettings(
            requestInterval = None,
            requestApproximateByteSize = None,
          ),
        ),
        TopologySettings(randomSourceToCreateSettings.nextLong()),
        // The purpose of this test is to make sure we progress time by making empty blocks. As such we don't want view
        // changes to happen (since they would also make the network do progress). So we need to explicitly check that
        // no ViewChanges happened.
        failOnViewChange = true,
      ),
    ),
  )
}

// Note that simulation tests don't use a real network, so this test doesn't cover gRPC messages.
class BftOrderingSimulationTest2NodesLargeRequests extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 1

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 2,
    stages = NonEmpty(
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
          clientSettings = ClientSettings(
            // The test is a bit slow with the default interval
            requestInterval = Some(10.seconds),
            requestApproximateByteSize =
              // -100 to account for tags and payloads' prefixes
              // Exceeding the default size results in warning logs and dropping messages in Mempool
              Some(PositiveInt.tryCreate(BftBlockOrdererConfig.DefaultMaxRequestPayloadBytes - 100)),
          ),
        ),
        TopologySettings(randomSourceToCreateSettings.nextLong()),
      ),
    ),
  )
}

class BftOrderingSimulationTest2NodesCrashFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 10

  private val durationOfFirstPhaseWithFaults = 2.minutes
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // remove seed to randomly explore seeds

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 2,
    stages = NonEmpty(
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
    ),
  )
}

class BftOrderingSimulationTest4NodesCrashFaults extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 5

  private val durationOfFirstPhaseWithFaults = 2.minutes
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // remove seed to randomly explore seeds

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 4,
    stages = NonEmpty(
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
    ),
  )
}

class BftOrderingSimulationTestOffboarding extends BftOrderingSimulationTest {
  override val numberOfRuns: Int = 4

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 1.minute

  private val randomSourceToCreateSettings: Random =
    new Random(4) // Manually remove the seed for fully randomized local runs.

  override def allowedWarnings: Seq[LogEntry => Assertion] = Seq(
    // We might get messages from off boarded nodes, don't count these as errors.
    { logEntry =>
      logEntry.message should include(
        "but it cannot be verified in the currently known dissemination topology"
      )
      logEntry.loggerName should include("AvailabilityModule")
    },
    { logEntry =>
      logEntry.message should include(
        "sent invalid ACK for batch"
      )
      logEntry.loggerName should include("AvailabilityModule")
    },
  )

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = 5,
    stages = NonEmpty(
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
    ),
  )
}
