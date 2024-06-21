// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.canton.data.Offset

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

  def toApiString(offset: Offset): Ref.LedgerString =
    offset.toHexString

  implicit class ApiOffsetConverter(val offset: Offset) {
    def toApiString: Ref.LedgerString = ApiOffset.toApiString(offset)
  }

}
