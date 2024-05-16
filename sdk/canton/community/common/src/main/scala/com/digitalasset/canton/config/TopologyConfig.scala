// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.TopologyConfig.*

import scala.concurrent.duration.Duration

final case class TopologyConfig(
    topologyTransactionRegistrationTimeout: NonNegativeDuration =
      defaultTopologyTransactionRegistrationTimeout
)

object TopologyConfig {
  private[TopologyConfig] val defaultTopologyTransactionRegistrationTimeout =
    NonNegativeDuration.ofSeconds(20)

  def NotUsed: TopologyConfig = TopologyConfig(topologyTransactionRegistrationTimeout =
    NonNegativeDuration.tryFromDuration(Duration.Inf)
  )
}
