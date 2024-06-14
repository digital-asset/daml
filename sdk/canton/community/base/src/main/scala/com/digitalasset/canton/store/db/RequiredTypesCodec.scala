// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt, PositiveLong}
import slick.jdbc.{GetResult, SetParameter}

object RequiredTypesCodec {
  def positiveLongDBDeserializer(l: Long): PositiveLong =
    PositiveLong.create(l).getOrElse {
      throw new DbDeserializationException(
        s"$l cannot be deserialized to a positive long"
      )
    }

  def positiveIntDBDeserializer(i: Int): PositiveInt =
    PositiveInt.create(i).getOrElse {
      throw new DbDeserializationException(
        s"$i cannot be deserialized to a positive int"
      )
    }

  implicit val positiveLongSetResult: SetParameter[PositiveLong] = (v: PositiveLong, pp) =>
    pp >> v.value

  implicit val positiveIntSetResult: SetParameter[PositiveInt] = (v: PositiveInt, pp) =>
    pp >> v.value

  implicit val positiveLongGetResult: GetResult[PositiveLong] =
    GetResult.GetLong.andThen(positiveLongDBDeserializer)

  implicit val positiveIntGetResult: GetResult[PositiveInt] =
    GetResult.GetInt.andThen(positiveIntDBDeserializer)

  def nonNegativeLongDBDeserializer(l: Long): NonNegativeLong =
    NonNegativeLong.create(l).valueOr { err =>
      throw new DbDeserializationException(
        s"$l cannot be deserialized to a non negative long: $err"
      )
    }

  implicit val nonNegativeLongSetResult: SetParameter[NonNegativeLong] = (v: NonNegativeLong, pp) =>
    pp >> v.value

  implicit val nonNegativeLongGetResult: GetResult[NonNegativeLong] =
    GetResult.GetLong.andThen(nonNegativeLongDBDeserializer)

  implicit val nonNegativeLongOptionGetResult: GetResult[Option[NonNegativeLong]] =
    GetResult.GetLongOption.andThen(_.map(nonNegativeLongDBDeserializer))

  implicit val positiveIntOptionGetResult: GetResult[Option[PositiveInt]] =
    GetResult.GetIntOption.andThen(_.map(positiveIntDBDeserializer))

}
