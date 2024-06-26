// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.EventCostCalculator.{
  EnvelopeCostDetails,
  EventCostDetails,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

object EventCostCalculator {

  /** Contains details of the computation of the cost of an envelope.
    * @param writeCost write cost associated with the envelope
    * @param readCost read cost associated with the envelope
    * @param finalCost final cost associated with the envelope (typically writeCost + readCost at the moment)
    * @param recipients recipients of the envelope
    */
  final case class EnvelopeCostDetails(
      writeCost: Long,
      readCost: Long,
      finalCost: Long,
      recipients: NonEmpty[Seq[Recipient]],
  ) extends PrettyPrinting {

    override def pretty: Pretty[EnvelopeCostDetails] = prettyOfClass(
      param("write cost", _.writeCost),
      param("read cost", _.readCost),
      param("final cost", _.finalCost),
      param("recipients", _.recipients),
    )
  }

  /** Contains details of the computation of the cost of an event
    * @param costMultiplier cost multiplier used for the computation
    * @param groupToMembersSize size of each recipient group
    * @param envelopes details of the cost computation of each envelope
    * @param eventCost final cost of the event
    */
  final case class EventCostDetails(
      costMultiplier: PositiveInt,
      groupToMembersSize: Map[GroupRecipient, Int],
      envelopes: List[EnvelopeCostDetails],
      eventCost: NonNegativeLong,
  ) extends PrettyPrinting {

    override def pretty: Pretty[EventCostDetails] = prettyOfClass(
      param("cost multiplier", _.costMultiplier),
      param("group to members size", _.groupToMembersSize),
      param("envelopes cost details", _.envelopes),
      param("event cost", _.eventCost),
    )
  }
}

// TODO(i12907): Precise costs calculations
class EventCostCalculator(override val loggerFactory: NamedLoggerFactory) extends NamedLogging {

  def computeEventCost(
      event: Batch[ClosedEnvelope],
      costMultiplier: PositiveInt,
      groupToMembers: Map[GroupRecipient, Set[Member]],
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): EventCostDetails = {
    // If changing the cost computation, make sure to tie it to a protocol version
    // For now there's only one version of cost computation
    if (protocolVersion >= ProtocolVersion.v31) {
      val envelopeCosts = event.envelopes.map(computeEnvelopeCost(costMultiplier, groupToMembers))
      val eventCost = NonNegativeLong.tryCreate(envelopeCosts.map(_.finalCost).sum)
      EventCostDetails(
        costMultiplier,
        groupToMembers.view.mapValues(_.size).toMap,
        envelopeCosts,
        eventCost,
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
  )(envelope: ClosedEnvelope): EnvelopeCostDetails = {
    val writeCost = payloadSize(envelope).toLong

    val allRecipients: NonEmpty[Seq[Recipient]] = envelope.recipients.allRecipients.toSeq
    val recipientsSize = allRecipients.map {
      case recipient: GroupRecipient => groupToMembers.get(recipient).map(_.size).getOrElse(0)
      case _: MemberRecipient => 1
    }.sum

    // read costs are based on the write costs and multiplied by the number of recipients with a readVsWrite cost multiplier
    try {
      // `writeCosts` and `recipientsSize` are originally Int, so multiplying them together cannot overflow a long
      val readCost =
        math.multiplyExact(writeCost * recipientsSize.toLong, costMultiplier.value.toLong) / 10000
      val finalCost = math.addExact(readCost, writeCost)
      EnvelopeCostDetails(
        writeCost = writeCost,
        readCost = readCost,
        finalCost = finalCost,
        allRecipients,
      )
    } catch {
      case _: ArithmeticException =>
        throw new IllegalStateException(
          s"""Overflow in cost computation:
           |  writeCosts = $writeCost
           |  recipientsSize = $recipientsSize
           |  costMultiplier = $costMultiplier""".stripMargin
        )
    }
  }
}
