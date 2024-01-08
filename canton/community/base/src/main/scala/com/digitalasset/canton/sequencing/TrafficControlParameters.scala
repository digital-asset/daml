// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0 as protoV0
import com.digitalasset.canton.sequencing.TrafficControlParameters.{
  DefaultBaseTrafficAmount,
  DefaultMaxBaseTrafficAccumulationDuration,
  DefaultReadVsWriteScalingFactor,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time

/** Traffic control configuration values - stored as dynamic domain parameters
  *
  * @param maxBaseTrafficAmount maximum amount of bytes per maxBaseTrafficAccumulationDuration acquired as "free" traffic per member
  * @param readVsWriteScalingFactor multiplier used to compute cost of an event. In per ten-mil (1 / 10 000). Defaults to 200 (=2%).
  *                                 A multiplier of 2% means the base cost will be increased by 2% to produce the effective cost.
  * @param maxBaseTrafficAccumulationDuration maximum amount of time the base rate traffic will accumulate before being capped
  */
final case class TrafficControlParameters(
    maxBaseTrafficAmount: NonNegativeLong = DefaultBaseTrafficAmount,
    readVsWriteScalingFactor: PositiveInt = DefaultReadVsWriteScalingFactor,
    maxBaseTrafficAccumulationDuration: time.NonNegativeFiniteDuration =
      DefaultMaxBaseTrafficAccumulationDuration,
) extends PrettyPrinting {
  lazy val baseRate: NonNegativeLong =
    NonNegativeLong.tryCreate(
      maxBaseTrafficAmount.value / maxBaseTrafficAccumulationDuration.unwrap.toSeconds
    )

  def toProtoV0: protoV0.TrafficControlParameters = protoV0.TrafficControlParameters(
    maxBaseTrafficAmount.value,
    Some(maxBaseTrafficAccumulationDuration.toProtoPrimitive),
    readVsWriteScalingFactor.value,
  )

  override def pretty: Pretty[TrafficControlParameters] = prettyOfClass(
    param("max base traffic amount", _.maxBaseTrafficAmount),
    param("read vs write scaling factor", _.readVsWriteScalingFactor),
    param("max base traffic accumulation duration", _.maxBaseTrafficAccumulationDuration),
  )
}

object TrafficControlParameters {
  // Default is computed such that 10 txs of 20KB can be sequenced during the max traffic accumulation window
  val DefaultBaseTrafficAmount: NonNegativeLong = NonNegativeLong.tryCreate(10 * 20 * 1024)
  val DefaultReadVsWriteScalingFactor: PositiveInt =
    PositiveInt.tryCreate(200)
  val DefaultMaxBaseTrafficAccumulationDuration: time.NonNegativeFiniteDuration =
    time.NonNegativeFiniteDuration.apply(time.PositiveSeconds.tryOfMinutes(10L))

  def fromProtoV0(
      proto: protoV0.TrafficControlParameters
  ): ParsingResult[TrafficControlParameters] = {
    for {
      maxBaseTrafficAmount <- ProtoConverter.parseNonNegativeLong(proto.maxBaseTrafficAmount)
      maxBaseTrafficAccumulationDuration <- ProtoConverter.parseRequired(
        time.NonNegativeFiniteDuration.fromProtoPrimitive("max_base_traffic_accumulation_duration"),
        "max_base_traffic_accumulation_duration",
        proto.maxBaseTrafficAccumulationDuration,
      )
      scalingFactor <- ProtoConverter.parsePositiveInt(proto.readVsWriteScalingFactor)
    } yield TrafficControlParameters(
      maxBaseTrafficAmount,
      scalingFactor,
      maxBaseTrafficAccumulationDuration,
    )
  }
}
