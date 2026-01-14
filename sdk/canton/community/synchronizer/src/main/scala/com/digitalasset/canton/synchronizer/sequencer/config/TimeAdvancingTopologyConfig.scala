// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.NonNegativeFiniteDuration

import TimeAdvancingTopologyConfig.*

/** Configures the timeouts for time advancement broadcast messages that sequencers send after a
  * topology transaction.
  *
  * @param maxSequencingTimeWindow
  *   The difference between the effective timestamp of the topology transaction and the max
  *   sequencing time for the time advancement broadcast message. The time advancement broadcast
  *   must be sequenced within this time window.
  * @param gracePeriod
  *   Waiting period (measured on the local sequencer clock) on top of the topology change delay for
  *   the sequencer before it sends a broadcast message. If it processes another broadcast or
  *   topology transaction message during this waiting time whose sequencing time is after the
  *   effective timestamp, it will not send a broadcast message. A short grace period leads to
  *   faster observation of the topology transaction becoming effective; a long grace period reduces
  *   the number of broadcast messages sent.
  * @param sequencingPatience
  *   How long the sequencer waits before it resends a broadcast message for a topology change.
  *   Should be at least the expected latency of ordering a message.
  * @param pollBackoff
  *   How frequently the sequencer should check whether it should send a broadcast message. Should
  *   be lower than the [[sequencingPatience]] and the [[gracePeriod]].
  */
final case class TimeAdvancingTopologyConfig(
    maxSequencingTimeWindow: NonNegativeFiniteDuration = defaultMaxSequencingTimeWindow,
    gracePeriod: NonNegativeFiniteDuration = defaultGracePeriod,
    sequencingPatience: NonNegativeFiniteDuration = defaultSequencingPatience,
    pollBackoff: NonNegativeFiniteDuration = defaultPollBackoff,
)

object TimeAdvancingTopologyConfig {
  val defaultMaxSequencingTimeWindow: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(30)

  val defaultGracePeriod: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMillis(500)

  val defaultSequencingPatience: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(1)

  val defaultPollBackoff: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMillis(250)
}
