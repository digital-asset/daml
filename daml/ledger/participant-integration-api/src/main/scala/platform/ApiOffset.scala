// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref

import scala.util.{Failure, Success, Try}

// This utility object is used as a single point to encode and decode
// offsets sent over the API and received from the API.
private[daml] object ApiOffset {

  val begin: Offset = Offset.fromByteArray(Array(0: Byte))

  def fromString(s: String): Try[Offset] =
    Ref.HexString
      .fromString(s)
      .fold(
        err => Failure(new IllegalArgumentException(err)),
        b => Success(Offset.fromHexString(b)),
      )

  def assertFromString(s: String): Offset = fromString(s).get

  def toApiString(offset: Offset): Ref.LedgerString =
    offset.toHexString

  implicit class ApiOffsetConverter(val offset: Offset) {
    def toApiString: Ref.LedgerString = ApiOffset.toApiString(offset)
  }

}
