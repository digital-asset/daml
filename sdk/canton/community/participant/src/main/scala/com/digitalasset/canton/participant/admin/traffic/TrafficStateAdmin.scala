// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.traffic

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.v30.TrafficState as ProtocolTrafficState
import com.digitalasset.canton.sequencing.protocol.TrafficState
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*

/** Conversion utilities between the sequencing TrafficState and the participant admin API TrafficState protos
  */
object TrafficStateAdmin {
  implicit val nonNegativeLongToLong: Transformer[NonNegativeLong, Long] = _.value
  implicit val positiveIntToInt: Transformer[PositiveInt, Int] = _.value
  implicit val cantonTimestampToProtoTimestamp: Transformer[CantonTimestamp, Long] =
    _.toProtoPrimitive
  def toProto(trafficState: TrafficState): v30.TrafficState =
    trafficState.transformInto[v30.TrafficState]
  def fromProto(trafficState: v30.TrafficState): Either[ProtoDeserializationError, TrafficState] =
    TrafficState.fromProtoV30(trafficState.transformInto[ProtocolTrafficState])
}
