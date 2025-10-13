// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.TopologyConfig.*
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation

/** @param topologyTransactionRegistrationTimeout
  *   Used to determine the max sequencing time for topology transaction broadcasts.
  * @param topologyTransactionObservationTimeout
  *   Determines up to how long the node waits for observing the dispatched topology transactions in
  *   its own local synchronizer store. The observation timeout is checked against the node's wall
  *   clock. After this timeout, the node fails the dispatch cycle. This timeout is only triggered,
  *   if the sequencer accepts the topology transaction broadcast submission request, but drops the
  *   message during ordering (for whatever reason).
  * @param broadcastBatchSize
  *   The maximum number of topology transactions sent in a topology transaction broadcast
  * @param broadcastRetryDelay
  *   The delay after which a failed dispatch cycle will be triggered again.
  * @param validateInitialTopologySnapshot
  *   Whether or not the node will validate the initial topology snapshot when onboarding to a
  *   synchronizer.
  * @param disableOptionalTopologyChecks
  *   if true (default is false), don't run the optional checks which prevent accidental damage to
  *   this node
  */
final case class TopologyConfig(
    topologyTransactionRegistrationTimeout: NonNegativeFiniteDuration =
      defaultTopologyTransactionRegistrationTimeout,
    topologyTransactionObservationTimeout: NonNegativeFiniteDuration =
      defaultTopologyTransactionObservationTimeout,
    broadcastBatchSize: PositiveInt = defaultBroadcastBatchSize,
    broadcastRetryDelay: NonNegativeFiniteDuration = defaultBroadcastRetryDelay,
    validateInitialTopologySnapshot: Boolean = true,
    disableOptionalTopologyChecks: Boolean = false,
) extends UniformCantonConfigValidation

object TopologyConfig {
  implicit val topologyConfigCantonConfigValidator: CantonConfigValidator[TopologyConfig] = {
    import CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[TopologyConfig]
  }

  private[TopologyConfig] val defaultTopologyTransactionRegistrationTimeout =
    NonNegativeFiniteDuration.ofSeconds(20)

  private[TopologyConfig] val defaultTopologyTransactionObservationTimeout =
    NonNegativeFiniteDuration.ofSeconds(30)

  private[TopologyConfig] val defaultBroadcastRetryDelay =
    NonNegativeFiniteDuration.ofSeconds(10)

  private[TopologyConfig] val defaultBroadcastBatchSize: PositiveInt = PositiveInt.tryCreate(100)

  def NotUsed: TopologyConfig = TopologyConfig(topologyTransactionRegistrationTimeout =
    NonNegativeFiniteDuration(NonNegativeDuration.maxTimeout)
  )
}
