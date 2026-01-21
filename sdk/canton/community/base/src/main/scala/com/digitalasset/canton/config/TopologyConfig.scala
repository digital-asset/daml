// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.TopologyConfig.*
import com.google.common.annotations.VisibleForTesting

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
  *   if false, the validation is skipped and the snapshot is directly imported. this is risky as it
  *   might create a fork if the validation was changed. therefore, we only use this with great
  *   care. the proper solution is to make validation so fast that it doesn't impact performance.
  * @param disableOptionalTopologyChecks
  *   if true (default is false), don't run the optional checks which prevent accidental damage to
  *   this node
  * @param dispatchQueueBackpressureLimit
  *   new topology requests will be backpressured if the number of existing requests exceeds this
  *   number
  * @param useTimeProofsToObserveEffectiveTime
  *   Whether the node will use time proofs to observe when an effective time has been reached. If
  *   false, no time proofs will be sent to the sequencers by any Canton node.
  * @param maxTopologyStateCacheItems
  *   The maximum number of UIDs + namespaces that can be cached in the write through cache
  * @param enableTopologyStateCacheConsistencyChecks
  *   If true, the topology state cache runs additional consistency checks. This is costly and
  *   should not be enabled in production environments.
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
    dispatchQueueBackpressureLimit: NonNegativeInt = defaultMaxUnsentTopologyQueueSize,
    useTimeProofsToObserveEffectiveTime: Boolean = false,
    maxTopologyStateCacheItems: PositiveInt = defaultTopologyStateWriteThroughCacheSize,
    enableTopologyStateCacheConsistencyChecks: Boolean = false,
)

object TopologyConfig {

  private[TopologyConfig] val defaultMaxUnsentTopologyQueueSize: NonNegativeInt =
    NonNegativeInt.tryCreate(100)

  private[TopologyConfig] val defaultTopologyTransactionRegistrationTimeout =
    NonNegativeFiniteDuration.ofSeconds(20)

  private[TopologyConfig] val defaultTopologyTransactionObservationTimeout =
    NonNegativeFiniteDuration.ofSeconds(30)

  private[TopologyConfig] val defaultBroadcastRetryDelay =
    NonNegativeFiniteDuration.ofSeconds(10)

  private[TopologyConfig] val defaultBroadcastBatchSize: PositiveInt = PositiveInt.tryCreate(100)

  private[TopologyConfig] val defaultTopologyStateWriteThroughCacheSize: PositiveInt =
    PositiveInt.tryCreate(1000)

  @VisibleForTesting
  val forTesting: TopologyConfig = TopologyConfig(
    maxTopologyStateCacheItems = PositiveInt.tryCreate(10),
    enableTopologyStateCacheConsistencyChecks = true,
  )

  def NotUsed: TopologyConfig = TopologyConfig(topologyTransactionRegistrationTimeout =
    NonNegativeFiniteDuration(NonNegativeDuration.maxTimeout)
  )
}
