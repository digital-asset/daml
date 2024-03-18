// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.apply.*
import com.digitalasset.canton.config.RequireTypes
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveLong}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.store.db.RequiredTypesCodec.nonNegativeLongOptionGetResult
import slick.jdbc.{GetResult, SetParameter}

/** Traffic state stored in the sequencer per event needed for enforcing traffic control */
final case class TrafficState(
    extraTrafficRemainder: NonNegativeLong,
    extraTrafficConsumed: NonNegativeLong,
    baseTrafficRemainder: NonNegativeLong,
    timestamp: CantonTimestamp,
) extends PrettyPrinting {
  lazy val extraTrafficLimit: Option[PositiveLong] =
    PositiveLong.create((extraTrafficRemainder + extraTrafficConsumed).value).toOption

  def update(
      newExtraTrafficLimit: NonNegativeLong,
      timestamp: CantonTimestamp,
  ): Either[RequireTypes.InvariantViolation, TrafficState] = {
    NonNegativeLong.create(newExtraTrafficLimit.value - extraTrafficConsumed.value).map {
      newRemainder =>
        this.copy(
          timestamp = timestamp,
          extraTrafficRemainder = newRemainder,
        )
    }
  }

  def toSequencedEventTrafficState: SequencedEventTrafficState = SequencedEventTrafficState(
    extraTrafficRemainder = extraTrafficRemainder,
    extraTrafficConsumed = extraTrafficConsumed,
  )

  override def pretty: Pretty[TrafficState] = prettyOfClass(
    param("timestamp", _.timestamp),
    param("extra traffic remainder", _.extraTrafficRemainder),
    param("extra traffic consumed", _.extraTrafficConsumed),
    param("base traffic remainder", _.baseTrafficRemainder),
  )
}

object TrafficState {

  implicit val setResultParameter: SetParameter[TrafficState] = { (v: TrafficState, pp) =>
    pp >> Some(v.extraTrafficRemainder.value)
    pp >> Some(v.extraTrafficConsumed.value)
    pp >> Some(v.baseTrafficRemainder.value)
    pp >> v.timestamp
  }

  implicit val getResultTrafficState: GetResult[Option[TrafficState]] = {
    GetResult
      .createGetTuple4(
        nonNegativeLongOptionGetResult,
        nonNegativeLongOptionGetResult,
        nonNegativeLongOptionGetResult,
        CantonTimestamp.getResultOptionTimestamp,
      )
      .andThen(_.mapN(TrafficState.apply))
  }

  def empty(timestamp: CantonTimestamp): TrafficState = TrafficState(
    NonNegativeLong.zero,
    NonNegativeLong.zero,
    NonNegativeLong.zero,
    timestamp,
  )
}
