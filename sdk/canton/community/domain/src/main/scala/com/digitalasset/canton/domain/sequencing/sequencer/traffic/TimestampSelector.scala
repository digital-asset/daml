// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.TrafficControlStateRequest.{
  TimestampSelector as TimestampSelectorP
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

object TimestampSelector {
  sealed trait TimestampSelector {
    def toProtoV30: v30.TrafficControlStateRequest.TimestampSelector = this match {
      case ExactTimestamp(timestamp) =>
        TimestampSelectorP.ExactTimestamp(timestamp.toProtoPrimitive)
      case LastUpdatePerMember =>
        TimestampSelectorP.RelativeTimestamp(
          v30.TrafficControlStateRequest.RelativeTimestamp.RELATIVE_TIMESTAMP_LAST_UPDATE_PER_MEMBER
        )
      case LatestSafe =>
        TimestampSelectorP.RelativeTimestamp(
          v30.TrafficControlStateRequest.RelativeTimestamp.RELATIVE_TIMESTAMP_LATEST_SAFE_UNSPECIFIED
        )
      case LatestApproximate =>
        TimestampSelectorP.RelativeTimestamp(
          v30.TrafficControlStateRequest.RelativeTimestamp.RELATIVE_TIMESTAMP_LATEST_APPROXIMATE
        )
    }
  }

  final case class ExactTimestamp(timestamp: CantonTimestamp) extends TimestampSelector
  final case object LastUpdatePerMember extends TimestampSelector
  final case object LatestSafe extends TimestampSelector
  final case object LatestApproximate extends TimestampSelector

  def fromProtoV30(
      selectorP: v30.TrafficControlStateRequest.TimestampSelector
  ): ParsingResult[TimestampSelector] = {
    selectorP match {
      // Empty defaults to LatestSafe
      case TimestampSelectorP.Empty => Right(LatestSafe)
      case TimestampSelectorP.ExactTimestamp(value) =>
        CantonTimestamp.fromProtoPrimitive(value).map(ExactTimestamp)
      case TimestampSelectorP.RelativeTimestamp(
            v30.TrafficControlStateRequest.RelativeTimestamp.RELATIVE_TIMESTAMP_LATEST_SAFE_UNSPECIFIED
          ) =>
        Right(LatestSafe)
      case TimestampSelectorP.RelativeTimestamp(
            v30.TrafficControlStateRequest.RelativeTimestamp.RELATIVE_TIMESTAMP_LAST_UPDATE_PER_MEMBER
          ) =>
        Right(LastUpdatePerMember)
      case TimestampSelectorP.RelativeTimestamp(
            v30.TrafficControlStateRequest.RelativeTimestamp.RELATIVE_TIMESTAMP_LATEST_APPROXIMATE
          ) =>
        Right(LatestApproximate)
      case TimestampSelectorP.RelativeTimestamp(
            v: v30.TrafficControlStateRequest.RelativeTimestamp.Unrecognized
          ) =>
        Left(ProtoDeserializationError.UnrecognizedEnum("relative_timestamp", v.unrecognizedValue))
    }
  }
}
