// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveLong}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import slick.jdbc.{GetResult, SetParameter}

final case class GlobalOffset(v: PositiveLong) extends Ordered[GlobalOffset] with PrettyPrinting {
  def unwrap: PositiveLong = v

  def toLong: Long = v.unwrap
  def toNonNegative: NonNegativeLong = v.toNonNegative

  override def compare(that: GlobalOffset): Int = this.toLong.compare(that.toLong)

  override def pretty: Pretty[GlobalOffset.this.type] = prettyOfString(_ => v.toString)

  def min(other: GlobalOffset): GlobalOffset = GlobalOffset(
    PositiveLong.tryCreate(v.unwrap.min(other.toLong))
  )
  def max(other: GlobalOffset): GlobalOffset = GlobalOffset(
    PositiveLong.tryCreate(v.unwrap.max(other.toLong))
  )

  def increment: GlobalOffset = GlobalOffset.tryFromLong(v.unwrap + 1)

  def +(i: PositiveLong): GlobalOffset = new GlobalOffset(v + i)
  def +(i: NonNegativeInt): GlobalOffset = new GlobalOffset(v.tryAdd(i.unwrap.toLong))
}

object GlobalOffset {
  implicit val getResultGlobalOffset: GetResult[GlobalOffset] =
    GetResult(_.nextLong()).andThen(i => GlobalOffset(PositiveLong.tryCreate(i)))
  implicit val getResultGlobalOffsetO: GetResult[Option[GlobalOffset]] =
    GetResult(_.nextLongOption().map(GlobalOffset.tryFromLong))

  implicit val setParameterGlobalOffset: SetParameter[GlobalOffset] = (v, pp) => pp >> v.unwrap
  implicit val setParameterGlobalOffsetO: SetParameter[Option[GlobalOffset]] = (v, pp) =>
    pp >> v.map(_.unwrap.unwrap)

  val MaxValue: GlobalOffset = GlobalOffset(PositiveLong.MaxValue)

  def tryFromLong(i: Long): GlobalOffset =
    fromLong(i).valueOr(err => throw new IllegalArgumentException(err))

  def fromLong(i: Long): Either[String, GlobalOffset] = PositiveLong
    .create(i)
    .bimap(_ => s"Expecting positive value for global offset; found $i", GlobalOffset(_))
}
