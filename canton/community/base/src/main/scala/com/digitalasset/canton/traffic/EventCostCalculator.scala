// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.sequencing.protocol.{Batch, ClosedEnvelope}

// TODO(i12907): Precise costs calculations
class EventCostCalculator {

  def computeEventCost(
      event: Batch[ClosedEnvelope],
      costMultiplier: PositiveInt,
  ): NonNegativeLong = {
    NonNegativeLong.tryCreate(event.envelopes.map(computeEnvelopeCost(costMultiplier)).sum)
  }

  def computeEnvelopeCost(
      costMultiplier: PositiveInt
  )(envelope: ClosedEnvelope): Long = {
    val writeCosts = envelope.bytes.size()

    // read costs are based on the write costs and multiplied by the number of recipients with a readVsWrite cost multiplier
    val readCosts =
      writeCosts * envelope.recipients.allRecipients.size * costMultiplier.value / 10000L

    writeCosts + readCosts
  }
}
