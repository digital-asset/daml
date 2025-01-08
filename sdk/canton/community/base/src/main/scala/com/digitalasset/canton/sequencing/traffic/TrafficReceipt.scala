// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30.TrafficReceipt as TrafficReceiptP
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member

/** Traffic receipt sent with the deliver / deliver error receipt to the sender.
  * Contains updated traffic information after the event has been sequenced.
  * @param consumedCost cost consumed by the event
  * @param extraTrafficConsumed extra traffic consumed at this sequencing timestamp
  * @param baseTrafficRemainder base traffic remaining at this sequencing timestamp
  */
final case class TrafficReceipt(
    consumedCost: NonNegativeLong,
    extraTrafficConsumed: NonNegativeLong,
    baseTrafficRemainder: NonNegativeLong,
) extends PrettyPrinting {

  override protected def pretty: Pretty[TrafficReceipt] =
    prettyOfClass(
      param("consumed cost", _.consumedCost),
      param("extra traffic consumed", _.extraTrafficConsumed),
      param("base traffic remainder", _.baseTrafficRemainder),
    )

  def toProtoV30: TrafficReceiptP =
    TrafficReceiptP(
      consumedCost = consumedCost.value,
      extraTrafficConsumed = extraTrafficConsumed.value,
      baseTrafficRemainder = baseTrafficRemainder.value,
    )

  def toTrafficConsumed(member: Member, sequencingTimestamp: CantonTimestamp): TrafficConsumed =
    TrafficConsumed(
      member,
      sequencingTimestamp,
      extraTrafficConsumed = extraTrafficConsumed,
      baseTrafficRemainder = baseTrafficRemainder,
      lastConsumedCost = consumedCost,
    )
}

object TrafficReceipt {
  def fromProtoV30(trafficReceiptP: TrafficReceiptP): ParsingResult[TrafficReceipt] =
    for {
      consumedCost <- ProtoConverter.parseNonNegativeLong(
        "consumed_cost",
        trafficReceiptP.consumedCost,
      )
      totalExtraTrafficConsumed <- ProtoConverter.parseNonNegativeLong(
        "extra_traffic_consumed",
        trafficReceiptP.extraTrafficConsumed,
      )
      baseTrafficRemainder <- ProtoConverter.parseNonNegativeLong(
        "base_traffic_remainder",
        trafficReceiptP.baseTrafficRemainder,
      )
    } yield TrafficReceipt(
      consumedCost = consumedCost,
      extraTrafficConsumed = totalExtraTrafficConsumed,
      baseTrafficRemainder = baseTrafficRemainder,
    )
}
