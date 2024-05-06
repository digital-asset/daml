// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  GroupRecipient,
  MemberRecipient,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

// TODO(i12907): Precise costs calculations
class EventCostCalculator(override val loggerFactory: NamedLoggerFactory) extends NamedLogging {

  def computeEventCost(
      event: Batch[ClosedEnvelope],
      costMultiplier: PositiveInt,
      groupToMembers: Map[GroupRecipient, Set[Member]],
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): NonNegativeLong = {
    // If changing the cost computation, make sure to tie it to a protocol version
    // For now there's only one version of cost computation
    if (protocolVersion >= ProtocolVersion.v31) {
      NonNegativeLong.tryCreate(
        event.envelopes.map(computeEnvelopeCost(costMultiplier, groupToMembers)).sum
      )
    } else {
      ErrorUtil.invalidState(
        s"Traffic control is not supported for protocol version $protocolVersion"
      )
    }
  }

  @VisibleForTesting
  protected def payloadSize(envelope: ClosedEnvelope): Int = envelope.bytes.size()

  @VisibleForTesting
  def computeEnvelopeCost(
      costMultiplier: PositiveInt,
      groupToMembers: Map[GroupRecipient, Set[Member]],
  )(envelope: ClosedEnvelope): Long = {
    val writeCosts = payloadSize(envelope).toLong

    val allRecipients = envelope.recipients.allRecipients.toSeq
    val recipientsSize = allRecipients.map {
      case recipient: GroupRecipient => groupToMembers.get(recipient).map(_.size).getOrElse(0)
      case _: MemberRecipient => 1
    }.sum

    // read costs are based on the write costs and multiplied by the number of recipients with a readVsWrite cost multiplier
    try {
      // `writeCosts` and `recipientsSize` are originally Int, so multiplying them together cannot overflow a long
      val readCosts =
        math.multiplyExact(writeCosts * recipientsSize.toLong, costMultiplier.value.toLong) / 10000
      math.addExact(readCosts, writeCosts)
    } catch {
      case _: ArithmeticException =>
        throw new IllegalStateException(
          s"""Overflow in cost computation:
           |  writeCosts = $writeCosts
           |  recipientsSize = $recipientsSize
           |  costMultiplier = $costMultiplier""".stripMargin
        )
    }
  }
}
