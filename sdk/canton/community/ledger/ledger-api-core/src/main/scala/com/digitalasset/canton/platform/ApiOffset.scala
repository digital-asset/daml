// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.canton.data.Offset
import com.digitalasset.daml.lf.data.Ref

import scala.util.{Failure, Success, Try}

// This utility object is used as a single point to encode and decode
// offsets sent over the API and received from the API.
object ApiOffset {

  val begin: Offset = Offset.fromByteArray(Array(0: Byte))

  def tryFromString(s: String): Try[Offset] =
    fromString(s) match {
      case Left(msg) => Failure(new IllegalArgumentException(msg))
      case Right(offset) => Success(offset)
    }

  def fromString(s: String): Either[String, Offset] =
    Ref.HexString
      .fromString(s)
      .map(Offset.fromHexString)

  def assertFromString(s: String): Offset = tryFromString(s).fold(throw _, identity)

  // TODO(#21363) remove converter as it should be unused
  def assertFromStringToLongO(s: String): Option[Long] =
    Option.unless(s.isEmpty)(assertFromString(s).toLong)

  // TODO(#21363) remove converter as it should be unused
  def fromLongO(longO: Option[Long]): String =
    longO.map(fromLong).getOrElse("")

  // TODO(#21363) remove converter as it should be unused
  def fromLong(l: Long): String =
    Offset.fromLong(l).toHexString

  def toApiString(offset: Offset): Ref.HexString =
    offset.toHexString

  implicit class ApiOffsetConverter(val offset: Offset) {
    def toApiString: Ref.HexString = ApiOffset.toApiString(offset)
  }

}
