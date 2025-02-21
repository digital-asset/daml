// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.TopologyConfig.*
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation

import scala.concurrent.duration.Duration

/** @param topologyTransactionRegistrationTimeout
  *   Used to determine the max sequencing time for topology transaction broadcasts.
  * @param broadcastBatchSize
  *   The maximum number of topology transactions sent in a topology transaction broadcast
  * @param insecureIgnoreMissingExtraKeySignaturesInInitialSnapshot
  *   INSECURE: If set to true, the validation of the initial topology snapshot will not ignore
  *   missing signatures for extra keys (e.g. new signing keys for OwnerToKeyMapping) and not
  *   consider them required for the transaction to become fully authorized. This setting allows
  *   importing legacy topology snapshots that contain topology transactions that did not require
  *   signatures for new signing keys.
  */
final case class TopologyConfig(
    topologyTransactionRegistrationTimeout: NonNegativeDuration =
      defaultTopologyTransactionRegistrationTimeout,
    broadcastBatchSize: PositiveInt = defaultBroadcastBatchSize,
    insecureIgnoreMissingExtraKeySignaturesInInitialSnapshot: Boolean = false,
) extends UniformCantonConfigValidation

object TopologyConfig {
  implicit val topologyConfigCantonConfigValidator: CantonConfigValidator[TopologyConfig] = {
    import CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[TopologyConfig]
  }

  private[TopologyConfig] val defaultTopologyTransactionRegistrationTimeout =
    NonNegativeDuration.ofSeconds(20)

  val defaultBroadcastBatchSize = PositiveInt.tryCreate(100)

  def NotUsed: TopologyConfig = TopologyConfig(topologyTransactionRegistrationTimeout =
    NonNegativeDuration.tryFromDuration(Duration.Inf)
  )
}
