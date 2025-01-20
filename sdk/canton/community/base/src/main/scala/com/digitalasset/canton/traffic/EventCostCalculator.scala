// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  GroupRecipient,
  MemberRecipient,
}
import com.digitalasset.canton.topology.Member
import com.google.common.annotations.VisibleForTesting

// TODO(i12907): Precise costs calculations
class EventCostCalculator {

  def computeEventCost(
      event: Batch[ClosedEnvelope],
      costMultiplier: PositiveInt,
      groupToMembers: Map[GroupRecipient, Set[Member]],
  ): NonNegativeLong = {
    NonNegativeLong.tryCreate(
      event.envelopes.map(computeEnvelopeCost(costMultiplier, groupToMembers)).sum
    )
  }

  @VisibleForTesting
  protected def payloadSize(envelope: ClosedEnvelope): Int = envelope.bytes.size()

  def computeEnvelopeCost(
      costMultiplier: PositiveInt,
      groupToMembers: Map[GroupRecipient, Set[Member]],
  )(envelope: ClosedEnvelope): Long = {
    val writeCosts = payloadSize(envelope)

    val recipientsSize = envelope.recipients.allRecipients.toSeq.map {
      case recipient: GroupRecipient => groupToMembers.get(recipient).map(_.size).getOrElse(0)
      case _: MemberRecipient => 1
    }.sum

    // read costs are based on the write costs and multiplied by the number of recipients with a readVsWrite cost multiplier
    val readCosts = writeCosts * recipientsSize * costMultiplier.value / 10000L

    writeCosts + readCosts
  }
}
