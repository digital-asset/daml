// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.TrafficControlParameters as TrafficControlParametersInternal
import com.digitalasset.canton.time.PositiveFiniteDuration as InternalPositiveFiniteDuration

object TrafficControlParameters {
  private[canton] val default: TrafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = TrafficControlParametersInternal.DefaultBaseTrafficAmount,
    readVsWriteScalingFactor = TrafficControlParametersInternal.DefaultReadVsWriteScalingFactor,
    maxBaseTrafficAccumulationDuration =
      TrafficControlParametersInternal.DefaultMaxBaseTrafficAccumulationDuration.toConfig,
    setBalanceRequestSubmissionWindowSize =
      TrafficControlParametersInternal.DefaultSetBalanceRequestSubmissionWindowSize.toConfig,
    enforceRateLimiting = TrafficControlParametersInternal.DefaultEnforceRateLimiting,
    baseEventCost = TrafficControlParametersInternal.DefaultBaseEventCost,
    freeConfirmationResponses = TrafficControlParametersInternal.DefaultFreeConfirmationResponses,
  )
}

// TODO(#15650) Properly expose new BFT parameters and synchronizer limits
final case class TrafficControlParameters(
    maxBaseTrafficAmount: NonNegativeLong,
    readVsWriteScalingFactor: PositiveInt,
    maxBaseTrafficAccumulationDuration: config.PositiveFiniteDuration,
    setBalanceRequestSubmissionWindowSize: config.PositiveFiniteDuration,
    enforceRateLimiting: Boolean,
    baseEventCost: NonNegativeLong,
    freeConfirmationResponses: Boolean,
) extends PrettyPrinting {

  override protected def pretty: Pretty[TrafficControlParameters] = prettyOfClass(
    param("max base traffic amount", _.maxBaseTrafficAmount),
    param("read vs write scaling factor", _.readVsWriteScalingFactor),
    param("max base traffic accumulation duration", _.maxBaseTrafficAccumulationDuration),
    param("set balance request submission window size", _.setBalanceRequestSubmissionWindowSize),
    param("enforce rate limiting", _.enforceRateLimiting),
    param("base event cost", _.baseEventCost),
    param("free confirmation responses", _.freeConfirmationResponses),
  )

  private[canton] def toInternal: TrafficControlParametersInternal =
    TrafficControlParametersInternal(
      maxBaseTrafficAmount = maxBaseTrafficAmount,
      readVsWriteScalingFactor = readVsWriteScalingFactor,
      maxBaseTrafficAccumulationDuration =
        InternalPositiveFiniteDuration.fromConfig(maxBaseTrafficAccumulationDuration),
      setBalanceRequestSubmissionWindowSize =
        InternalPositiveFiniteDuration.fromConfig(setBalanceRequestSubmissionWindowSize),
      enforceRateLimiting = enforceRateLimiting,
      baseEventCost = baseEventCost,
      freeConfirmationResponses = freeConfirmationResponses,
    )
}
