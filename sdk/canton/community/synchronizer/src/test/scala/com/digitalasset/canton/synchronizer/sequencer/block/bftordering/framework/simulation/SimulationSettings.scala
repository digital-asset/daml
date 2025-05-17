// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.TopologySettings.{
  defaultKeyAdditionDistribution,
  defaultKeyExpirationDistribution,
}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

// following: https://github.com/DACH-NY/simulation-testing-demo/tree/main

final case class PowerDistribution(low: FiniteDuration, mean: FiniteDuration) {

  def generateRandomDuration(rng: Random): FiniteDuration = {
    // the nextDouble function has a range of [0, 1)
    val domain = rng.nextDouble() + Double.MinPositiveValue
    // the log function has a range of (-inf, 0] in the synchronizer of (0, 1]
    // so we negate to get the range of [0, inf)
    // 0 is excluded from the synchronizer to eliminate potential inf calculation blowing up FiniteDuration construction
    val sample = -Math.log(domain)
    // we adjust the mean, since we will add `low` afterwards
    // to guarantee we are at least `low`
    val adjustedMean = mean.minus(low).max(0.microseconds).toMicros

    FiniteDuration((adjustedMean * sample).toLong, TimeUnit.MICROSECONDS).plus(low)
  }

  def copyWithMaxLow(otherLow: FiniteDuration): PowerDistribution =
    this.copy(low = low.max(otherLow))

}

final case class Probability(prob: Double) {
  require(0 <= prob, "Probability must be at least 0")
  require(prob <= 1, "Probability must be at most 1")
  def flipCoin(rng: Random): Boolean =
    rng.nextDouble() <= prob
}

sealed trait PartitionMode {
  def selectSet[A](nodes: Set[A], random: Random): Set[A]
}

object PartitionMode {
  final case object None extends PartitionMode {
    override def selectSet[A](nodes: Set[A], random: Random): Set[A] = Set.empty
  }
  final case object UniformSize extends PartitionMode {
    override def selectSet[A](nodes: Set[A], random: Random): Set[A] = {
      val toTake = random.between(1, nodes.size + 1)
      random.shuffle(nodes).take(toTake)
    }
  }
  final case object IsolateSingle extends PartitionMode {
    override def selectSet[A](nodes: Set[A], random: Random): Set[A] = {
      val ix = random.between(0, nodes.size)
      val node = nodes.toSeq(ix)
      Set(node)
    }
  }
}

sealed trait PartitionSymmetry

object PartitionSymmetry {
  final case object Symmetric extends PartitionSymmetry
  final case object ASymmetric extends PartitionSymmetry
}

final case class NetworkSettings(
    randomSeed: Long,
    oneWayDelay: PowerDistribution = NetworkSettings.defaultRemoteMessageTimeDistribution,
    establishConnectionDelay: PowerDistribution =
      NetworkSettings.defaultRemoteMessageTimeDistribution,
    packetLoss: Probability = Probability(0),
    packetReplay: Probability = Probability(0),
    partitionMode: PartitionMode = PartitionMode.None,
    partitionSymmetry: PartitionSymmetry = PartitionSymmetry.Symmetric,
    partitionProbability: Probability = Probability(0),
    unPartitionProbability: Probability = Probability(0),
    partitionStability: FiniteDuration = 0.microseconds,
    unPartitionStability: FiniteDuration = 0.microseconds,
)

object NetworkSettings {
  private val defaultRemoteMessageTimeDistribution: PowerDistribution =
    PowerDistribution(10.milliseconds, 100.milliseconds)
}

final case class LocalSettings(
    randomSeed: Long,
    internalEventTimeDistribution: PowerDistribution =
      LocalSettings.defaultInternalEventTimeDistribution,
    futureTimeDistribution: PowerDistribution = LocalSettings.defaultFutureTimeDistribution,
    clockDriftChance: Probability = Probability(0),
    clockDrift: PowerDistribution = LocalSettings.defaultClockDriftDistribution,
    crashRestartChance: Probability = Probability(0),
    crashRestartGracePeriod: PowerDistribution = LocalSettings.defaultCrashRestartGracePeriod,
)

object LocalSettings {
  private val defaultInternalEventTimeDistribution: PowerDistribution =
    PowerDistribution(25.microseconds, 1.millisecond)

  private val defaultFutureTimeDistribution: PowerDistribution =
    PowerDistribution(1.millisecond, 20.milliseconds)

  private val defaultClockDriftDistribution: PowerDistribution =
    PowerDistribution(0.microseconds, 25.microseconds)

  private val defaultCrashRestartGracePeriod: PowerDistribution =
    PowerDistribution(1.second, 5.seconds)
}

final case class TopologySettings(
    randomSeed: Long,
    shouldDoKeyRotations: Boolean = false,
    keyExpirationDistribution: PowerDistribution = defaultKeyExpirationDistribution,
    keyAdditionDistribution: PowerDistribution = defaultKeyAdditionDistribution,
)

object TopologySettings {
  private val defaultKeyExpirationDistribution: PowerDistribution =
    PowerDistribution(10 seconds, 25 seconds)
  private val defaultKeyAdditionDistribution: PowerDistribution =
    PowerDistribution(10 seconds, 25 seconds)
}

final case class SimulationSettings(
    localSettings: LocalSettings,
    networkSettings: NetworkSettings,
    topologySettings: TopologySettings,
    durationOfFirstPhaseWithFaults: FiniteDuration,
    durationOfSecondPhaseWithoutFaults: FiniteDuration = 30.seconds,
    clientRequestInterval: Option[FiniteDuration] = Some(1.second),
    clientRequestApproximateByteSize: Option[PositiveInt] = Some(
      PositiveInt.three // fully arbitrary
    ),
    livenessCheckInterval: FiniteDuration = 20.seconds,
    nodeOnboardingDelays: Iterable[FiniteDuration] = Iterable.empty,
    becomingOnlineAfterOnboardingDelay: FiniteDuration =
      SimulationSettings.DefaultBecomingOnlineAfterOnboardingDelay,
    retryBecomingOnlineInterval: FiniteDuration = 1.second,
) {
  def totalSimulationTime: FiniteDuration =
    durationOfFirstPhaseWithFaults.plus(durationOfSecondPhaseWithoutFaults)
}

object SimulationSettings {

  val DefaultBecomingOnlineAfterOnboardingDelay: FiniteDuration = 15.seconds
}
