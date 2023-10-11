// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

object SequencerMemberRateLimiterResult {
  final case class TrafficAccepted()

  final case class AboveTrafficLimit(
      trafficCost: Long,
      extraTrafficRemainder: Long,
      remainingBaseTraffic: Long,
  )
}
