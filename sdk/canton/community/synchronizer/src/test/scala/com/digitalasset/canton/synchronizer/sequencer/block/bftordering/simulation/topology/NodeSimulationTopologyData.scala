// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{SigningKeyPair, SigningKeyUsage}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.TopologySettings
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.NodeSimulationTopologyData.SigningKeyPairWithLifetime

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Random

final case class NodeSimulationTopologyData(
    onboardingTime: TopologyActivationTime,
    signingKeyPair: SigningKeyPair,
    signingKeyPairExpiration: Option[CantonTimestamp],
    signingKeyPairWithLifetimes: Seq[SigningKeyPairWithLifetime] = Seq.empty,
) {
  // assert that there should always be at least one key valid
  signingKeyPairExpiration.foreach { firstKeyExpiration =>
    signingKeyPairWithLifetimes.foldLeft[Option[CantonTimestamp]](Some(firstKeyExpiration)) {
      case (maybeLatestExpirationTime, keyLifetime) =>
        maybeLatestExpirationTime match {
          case None => None
          case Some(currentLastTime) =>
            require(
              keyLifetime.addInstant <= currentLastTime,
              s"Node will have a period of time with no keys, last key expired $currentLastTime but next key is not added until ${keyLifetime.addInstant}",
            )
            keyLifetime.maybeRemoveInstant.map(_.max(currentLastTime))
        }
    }
  }

  private lazy val allKeys: Seq[SigningKeyPairWithLifetime] =
    SigningKeyPairWithLifetime(
      signingKeyPair,
      onboardingTime.value,
      signingKeyPairExpiration,
    ) +: signingKeyPairWithLifetimes

  def keysForTimestamp(currentTimestamp: CantonTimestamp): Seq[SigningKeyPair] =
    allKeys
      .filter(_.isValidAt(currentTimestamp))
      .map(_.keyPair)
}

object NodeSimulationTopologyData {
  final case class SigningKeyPairWithLifetime(
      keyPair: SigningKeyPair,
      addInstant: CantonTimestamp,
      maybeRemoveInstant: Option[CantonTimestamp],
  ) {
    maybeRemoveInstant.foreach(removeInstant => require(removeInstant.isAfter(addInstant)))

    def isValidAt(currentTimestamp: CantonTimestamp): Boolean =
      addInstant <= currentTimestamp && maybeRemoveInstant.forall(currentTimestamp <= _)
  }
}

final case class NodeSimulationTopologyDataFactory(
    maybeOnboardingDelay: Option[FiniteDuration], // None means available from start
    keyRotations: Seq[(FiniteDuration, Option[FiniteDuration])] = Seq.empty,
    initialKeyValidFor: Option[FiniteDuration] = None,
) {
  def toSimulationTopologyData(
      stageStart: CantonTimestamp,
      crypto: SymbolicCrypto,
  ): NodeSimulationTopologyData = {
    val onboardingTime = maybeOnboardingDelay match {
      case Some(onboardingDelay) =>
        TopologyActivationTime(stageStart.add(onboardingDelay.toJava))
      case None => Genesis.GenesisTopologyActivationTime
    }
    val signingKeyPair = crypto.newSymbolicSigningKeyPair(SigningKeyUsage.ProtocolOnly)
    val signingKeyPairExpiration =
      initialKeyValidFor.map(duration => stageStart.add(duration.toJava))
    val signingKeyPairWithLifetimes = keyRotations.map { case (addTime, expire) =>
      SigningKeyPairWithLifetime(
        crypto.newSymbolicSigningKeyPair(
          SigningKeyUsage.ProtocolOnly
        ),
        stageStart.add(addTime.toJava),
        expire.map(durationToExpire => stageStart.add(durationToExpire.toJava)),
      )
    }
    NodeSimulationTopologyData(
      onboardingTime,
      signingKeyPair,
      signingKeyPairExpiration,
      signingKeyPairWithLifetimes,
    )
  }
}

object NodeSimulationTopologyDataFactory {
  def generate(
      random: Random,
      onboardingTime: Option[FiniteDuration],
      topologySettings: TopologySettings,
      stopKeyRotations: FiniteDuration,
  ): NodeSimulationTopologyDataFactory = {

    val (
      keyExpire,
      keyRotations,
    ): (Option[FiniteDuration], Seq[(FiniteDuration, Option[FiniteDuration])]) =
      if (topologySettings.shouldDoKeyRotations) {
        val adjustForOnboardingTime = onboardingTime.getOrElse(0 seconds)
        val expire = topologySettings.keyExpirationDistribution
          .generateRandomDuration(random)
          .plus(adjustForOnboardingTime)
        val rotations: Seq[(FiniteDuration, Option[FiniteDuration])] = scala.collection.View
          .unfold((adjustForOnboardingTime, expire)) { case (lastRotation, lastExpired) =>
            if (lastExpired >= stopKeyRotations) {
              None
            } else {
              val keyAdded = lastExpired.min(
                lastRotation.plus(
                  topologySettings.keyAdditionDistribution.generateRandomDuration(random)
                )
              )
              val keyExpired = keyAdded.plus(
                topologySettings.keyExpirationDistribution.generateRandomDuration(random)
              )
              Some(
                (
                  keyAdded,
                  if (keyExpired >= stopKeyRotations) {
                    None
                  } else {
                    Some(keyExpired)
                  },
                ) -> (keyAdded, lastExpired.max(keyExpired))
              )
            }
          }
          .toSeq

        (if (expire >= stopKeyRotations) None else Some(expire), rotations)
      } else {
        (None, Seq.empty)
      }
    NodeSimulationTopologyDataFactory(
      onboardingTime,
      keyRotations,
      keyExpire,
    )
  }
}
