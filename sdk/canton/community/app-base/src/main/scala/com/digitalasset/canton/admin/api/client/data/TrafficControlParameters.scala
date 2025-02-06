// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.sequencing.TrafficControlParameters as TrafficControlParametersInternal
import com.digitalasset.canton.time.PositiveFiniteDuration as InternalPositiveFiniteDuration

// TODO(#15650) Properly expose new BFT parameters and synchronizer limits
final case class TrafficControlParameters(
    maxBaseTrafficAmount: NonNegativeLong,
    readVsWriteScalingFactor: PositiveInt,
    maxBaseTrafficAccumulationDuration: config.PositiveFiniteDuration,
    setBalanceRequestSubmissionWindowSize: config.PositiveFiniteDuration,
    enforceRateLimiting: Boolean,
) {

  private[canton] def toInternal: TrafficControlParametersInternal =
    TrafficControlParametersInternal(
      maxBaseTrafficAmount = maxBaseTrafficAmount,
      readVsWriteScalingFactor = readVsWriteScalingFactor,
      maxBaseTrafficAccumulationDuration =
        InternalPositiveFiniteDuration.fromConfig(maxBaseTrafficAccumulationDuration),
      setBalanceRequestSubmissionWindowSize =
        InternalPositiveFiniteDuration.fromConfig(setBalanceRequestSubmissionWindowSize),
      enforceRateLimiting = enforceRateLimiting,
    )
}
