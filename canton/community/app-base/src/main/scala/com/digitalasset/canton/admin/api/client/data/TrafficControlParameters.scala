// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.sequencing.{
  TrafficControlParameters as TrafficControlParametersInternal
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration as InternalNonNegativeFiniteDuration}

// TODO(#15650) Properly expose new BFT parameters and domain limits
final case class TrafficControlParameters(
    maxBaseTrafficAmount: NonNegativeLong,
    readVsWriteScalingFactor: PositiveInt,
    maxBaseTrafficAccumulationDuration: config.NonNegativeFiniteDuration,
) {

  private[canton] def toInternal: TrafficControlParametersInternal =
    TrafficControlParametersInternal(
      maxBaseTrafficAmount = maxBaseTrafficAmount,
      readVsWriteScalingFactor = readVsWriteScalingFactor,
      maxBaseTrafficAccumulationDuration =
        InternalNonNegativeFiniteDuration.fromConfig(maxBaseTrafficAccumulationDuration),
    )
}
