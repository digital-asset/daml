// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.canton.data.Offset
import com.digitalasset.daml.lf.data.Ref

import scala.util.{Failure, Success, Try}

// This utility object is used as a single point to encode and decode
// offsets sent over the API and received from the API.
object ApiOffset {

  def tryFromString(s: String): Try[Offset] =
    fromString(s) match {
      case Left(msg) => Failure(new IllegalArgumentException(msg))
      case Right(offset) => Success(offset)
    }

  private def fromString(s: String): Either[String, Offset] =
    Ref.HexString
      .fromString(s)
      .map(Offset.fromHexString)

  private def assertFromString(s: String): Offset = tryFromString(s).fold(throw _, identity)

  // TODO(#18685) remove converter as it should be unused
  def assertFromStringToLongO(s: String): Option[Long] =
    Option.unless(s.isEmpty)(assertFromString(s).toLong)

  // TODO(#18685) remove converter as it should be unused
  def assertFromStringToLong(s: String): Long =
    assertFromStringToLongO(s).getOrElse(0L)

  // TODO(#18685) remove converter as it should be unused
  def fromLong(l: Long): String =
    Offset.fromLong(l).toHexString
}
