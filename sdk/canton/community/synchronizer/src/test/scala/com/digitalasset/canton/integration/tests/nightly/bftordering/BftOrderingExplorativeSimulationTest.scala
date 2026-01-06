// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.bftordering

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.PartitionSymmetry.{
  ASymmetric,
  Symmetric,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.BftOrderingSimulationTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering.{
  SimulationTestSettings,
  SimulationTestStageSettings,
  TopologySettings,
}

import scala.collection.immutable.TreeMap
import scala.concurrent.duration.DurationInt
import scala.util.Random

class BftOrderingExplorativeSimulationTest extends BftOrderingSimulationTest {

  override def numberOfRuns: Int = 50

  private val randomSourceToCreateSettings = new Random()

  private val durationOfFirstPhaseWithFaults = 1.minute
  private val durationOfSecondPhaseWithoutFaults = 30.seconds

  private val zeroProbability: Probability = Probability(0)
  private def generateProb(low: Double, high: Double): Probability = Probability(
    randomSourceToCreateSettings.between(low, high)
  )

  private def randomWeightedNonEmptyOneOf[T](items: NonEmpty[Seq[(Int, T)]]): T = {
    // inspired by frequency from scalacheck
    // https://github.com/typelevel/scalacheck/blob/67a8463f4d092ddfc1233874fda707d74ae85353/core/shared/src/main/scala/org/scalacheck/Gen.scala#L907
    var total = 0L
    val builder = TreeMap.newBuilder[Long, T]
    items.foreach { case (weight, value) =>
      total += weight
      builder += ((total, value))
    }
    val tree = builder.result()

    val ix = randomSourceToCreateSettings.between(1, total + 1)
    tree.rangeFrom(ix).head._2
  }

  private def randomWeightedOneOf[T](head: (Int, T), tail: (Int, T)*): T =
    randomWeightedNonEmptyOneOf(
      NonEmpty.mk(Seq, head, tail*)
    )

  private def randomEquallyWeightedOneOf[T](head: T, tail: T*): T = randomWeightedNonEmptyOneOf(
    NonEmpty.mk(Seq, head, tail*).map(1 -> _)
  )

  private val shortTime: PowerDistribution = PowerDistribution(0.milliseconds, 100.milliseconds)
  private val longTime: PowerDistribution = PowerDistribution(1.second, 5.seconds)

  private def generateStage: SimulationTestStageSettings =
    SimulationTestStageSettings(
      simulationSettings = SimulationSettings(
        LocalSettings(
          randomSeed = randomSourceToCreateSettings.nextLong(),
          crashRestartChance = randomEquallyWeightedOneOf(
            zeroProbability,
            generateProb(0.0, 0.5),
          ),
          crashRestartGracePeriod = randomEquallyWeightedOneOf(
            shortTime,
            longTime,
          ),
        ),
        NetworkSettings(
          randomSeed = randomSourceToCreateSettings.nextLong(),
          packetLoss = randomEquallyWeightedOneOf(zeroProbability, generateProb(0.0, 0.33)),
          packetReplay = randomEquallyWeightedOneOf(zeroProbability, generateProb(0.0, 0.33)),
          partitionProbability =
            randomEquallyWeightedOneOf(generateProb(0.0, 0.2), generateProb(0.0, 0.5)),
          partitionMode = randomEquallyWeightedOneOf(
            PartitionMode.NoPartition,
            PartitionMode.UniformSize,
            PartitionMode.RandomlyDropConnections(generateProb(0, 0.5)),
          ),
          partitionStability = randomEquallyWeightedOneOf(2.seconds, 5.seconds, 10 seconds),
          partitionSymmetry = randomEquallyWeightedOneOf(Symmetric, ASymmetric),
          unPartitionProbability =
            randomEquallyWeightedOneOf(generateProb(0.0, 0.2), generateProb(0.0, 0.5)),
          unPartitionStability = randomEquallyWeightedOneOf(2.seconds, 5.seconds, 10 seconds),
        ),
        durationOfFirstPhaseWithFaults,
        durationOfSecondPhaseWithoutFaults,
      ),
      TopologySettings(
        randomSourceToCreateSettings.nextLong()
      ),
    )

  override def generateSettings: SimulationTestSettings = SimulationTestSettings(
    numberOfInitialNodes = randomEquallyWeightedOneOf(2, 4, 5),
    epochLength = EpochLength(
      randomWeightedOneOf[Long](
        10 -> 16L,
        5 -> 256L,
        2 -> randomSourceToCreateSettings.between(1L, 256L),
      )
    ),
    stages = NonEmpty(
      Seq,
      generateStage,
      generateStage,
    ),
  )
}
