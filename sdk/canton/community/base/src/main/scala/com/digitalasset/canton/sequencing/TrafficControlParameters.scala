// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30 as protoV30
import com.digitalasset.canton.sequencing.TrafficControlParameters.{
  DefaultBaseTrafficAmount,
  DefaultEnforceRateLimiting,
  DefaultMaxBaseTrafficAccumulationDuration,
  DefaultReadVsWriteScalingFactor,
  DefaultSetBalanceRequestSubmissionWindowSize,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time
import com.digitalasset.canton.time.PositiveFiniteDuration

/** Traffic control configuration values - stored as dynamic domain parameters
  *
  * @param maxBaseTrafficAmount maximum amount of bytes per maxBaseTrafficAccumulationDuration acquired as "free" traffic per member
  * @param readVsWriteScalingFactor multiplier used to compute cost of an event. In per ten-mil (1 / 10 000). Defaults to 200 (=2%).
  *                                 A multiplier of 2% means the base cost will be increased by 2% to produce the effective cost.
  * @param maxBaseTrafficAccumulationDuration maximum amount of time the base rate traffic will accumulate before being capped
  * @param setBalanceRequestSubmissionWindowSize time window used to compute the max sequencing time set for balance update requests
  *                                               The max sequencing time chosen will be the upper bound of the time window at which the request is submitted
  */
final case class TrafficControlParameters(
    maxBaseTrafficAmount: NonNegativeLong = DefaultBaseTrafficAmount,
    readVsWriteScalingFactor: PositiveInt = DefaultReadVsWriteScalingFactor,
    maxBaseTrafficAccumulationDuration: time.NonNegativeFiniteDuration =
      DefaultMaxBaseTrafficAccumulationDuration,
    setBalanceRequestSubmissionWindowSize: PositiveFiniteDuration =
      DefaultSetBalanceRequestSubmissionWindowSize,
    enforceRateLimiting: Boolean = DefaultEnforceRateLimiting,
) extends PrettyPrinting {
  lazy val baseRate: NonNegativeLong =
    NonNegativeLong.tryCreate(
      maxBaseTrafficAmount.value / maxBaseTrafficAccumulationDuration.unwrap.toSeconds
    )

  def toProtoV30: protoV30.TrafficControlParameters = protoV30.TrafficControlParameters(
    maxBaseTrafficAmount.value,
    Some(maxBaseTrafficAccumulationDuration.toProtoPrimitive),
    readVsWriteScalingFactor.value,
    Some(setBalanceRequestSubmissionWindowSize.toProtoPrimitive),
    enforceRateLimiting,
  )

  override def pretty: Pretty[TrafficControlParameters] = prettyOfClass(
    param("max base traffic amount", _.maxBaseTrafficAmount),
    param("read vs write scaling factor", _.readVsWriteScalingFactor),
    param("max base traffic accumulation duration", _.maxBaseTrafficAccumulationDuration),
    param("set balance request submission window size", _.setBalanceRequestSubmissionWindowSize),
    param("enforce rate limiting", _.enforceRateLimiting),
  )
}

object TrafficControlParameters {
  // Default is computed such that 10 txs of 20KB can be sequenced during the max traffic accumulation window
  val DefaultBaseTrafficAmount: NonNegativeLong = NonNegativeLong.tryCreate(10 * 20 * 1024)
  val DefaultReadVsWriteScalingFactor: PositiveInt =
    PositiveInt.tryCreate(200)
  val DefaultMaxBaseTrafficAccumulationDuration: time.NonNegativeFiniteDuration =
    time.NonNegativeFiniteDuration.apply(time.PositiveSeconds.tryOfMinutes(10L))
  val DefaultSetBalanceRequestSubmissionWindowSize: time.PositiveFiniteDuration =
    time.PositiveFiniteDuration.tryOfMinutes(4L)
  val DefaultEnforceRateLimiting: Boolean = true

  def fromProtoV30(
      proto: protoV30.TrafficControlParameters
  ): ParsingResult[TrafficControlParameters] = {
    for {
      maxBaseTrafficAmount <- ProtoConverter.parseNonNegativeLong(
        "max_base_traffic_amount",
        proto.maxBaseTrafficAmount,
      )
      maxBaseTrafficAccumulationDuration <- ProtoConverter.parseRequired(
        time.NonNegativeFiniteDuration.fromProtoPrimitive("max_base_traffic_accumulation_duration"),
        "max_base_traffic_accumulation_duration",
        proto.maxBaseTrafficAccumulationDuration,
      )
      setBalanceRequestSubmissionWindowSize <- ProtoConverter.parseRequired(
        time.PositiveFiniteDuration.fromProtoPrimitive(
          "set_balance_request_submission_window_size"
        ),
        "set_balance_request_submission_window_size",
        proto.setBalanceRequestSubmissionWindowSize,
      )
      scalingFactor <- ProtoConverter.parsePositiveInt(
        "read_vs_write_scaling_factor",
        proto.readVsWriteScalingFactor,
      )
    } yield TrafficControlParameters(
      maxBaseTrafficAmount,
      scalingFactor,
      maxBaseTrafficAccumulationDuration,
      setBalanceRequestSubmissionWindowSize,
      proto.enforceRateLimiting,
    )
  }
}
