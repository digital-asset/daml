// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.apply.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveLong}
import com.digitalasset.canton.domain.api.v30.SequencedEventTrafficState as SequencedEventTrafficStateP
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.RequiredTypesCodec.*
import slick.jdbc.GetResult

/** Traffic state stored alongside sequenced events
  */
final case class SequencedEventTrafficState(
    extraTrafficRemainder: NonNegativeLong,
    extraTrafficConsumed: NonNegativeLong,
) {
  lazy val extraTrafficLimit: Option[PositiveLong] =
    PositiveLong.create((extraTrafficRemainder + extraTrafficConsumed).value).toOption
  def toProtoV0: SequencedEventTrafficStateP = {
    SequencedEventTrafficStateP(
      extraTrafficRemainder = extraTrafficRemainder.value,
      extraTrafficConsumed = extraTrafficConsumed.value,
    )
  }

}

object SequencedEventTrafficState {
  def fromProtoV0(
      stateP: SequencedEventTrafficStateP
  ): ParsingResult[SequencedEventTrafficState] = {
    for {
      extraTrafficRemainder <- ProtoConverter.parseNonNegativeLong(stateP.extraTrafficRemainder)
      extraTrafficConsumed <- ProtoConverter.parseNonNegativeLong(stateP.extraTrafficConsumed)
    } yield SequencedEventTrafficState(
      extraTrafficRemainder = extraTrafficRemainder,
      extraTrafficConsumed = extraTrafficConsumed,
    )
  }

  implicit val sequencedEventTrafficStateGetResult: GetResult[Option[SequencedEventTrafficState]] =
    GetResult
      .createGetTuple2(
        nonNegativeLongOptionGetResult,
        nonNegativeLongOptionGetResult,
      )
      .andThen(_.mapN(SequencedEventTrafficState.apply))
}
