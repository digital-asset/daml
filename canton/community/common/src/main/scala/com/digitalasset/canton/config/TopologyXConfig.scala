// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.TopologyXConfig.*

import scala.concurrent.duration.Duration

final case class TopologyXConfig(
    topologyTransactionRegistrationTimeout: NonNegativeDuration =
      defaultTopologyTransactionRegistrationTimeout,
    // temporary flag to ease migration to topology validation being turned on
    enableTopologyTransactionValidation: Boolean = true,
)

object TopologyXConfig {
  private[TopologyXConfig] val defaultTopologyTransactionRegistrationTimeout =
    NonNegativeDuration.ofSeconds(20)

  def NotUsed: TopologyXConfig = TopologyXConfig(topologyTransactionRegistrationTimeout =
    NonNegativeDuration.tryFromDuration(Duration.Inf)
  )
}
