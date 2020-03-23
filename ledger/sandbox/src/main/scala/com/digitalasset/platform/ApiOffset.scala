// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform

import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.{Bytes, Ref}

import scala.util.{Failure, Success, Try}

object ApiOffset {

  def fromString(s: String): Try[Offset] =
    Bytes
      .fromString(s)
      .fold(
        err => Failure(new IllegalArgumentException(err)),
        b => Success(Offset(b))
      )

  def assertFromString(s: String): Offset =
    Offset(Bytes.assertFromString(s))

  def toApiString(offset: Offset): Ref.LedgerString =
    Offset.unwrap(offset).toHexString

  implicit class ApiOffsetConverter(val offset: Offset) {
    def toApiString: Ref.LedgerString = ApiOffset.toApiString(offset)
  }

}
