// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform

import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Ref
import com.google.common.io.BaseEncoding

import scala.util.Try

object ApiOffset {

  def fromString(s: String): Try[Offset] = Try {
    Offset.fromBytes(BaseEncoding.base16.decode(s))
  }

  def assertFromString(s: String): Offset = fromString(s).get

  def toApiString(offset: Offset): Ref.LedgerString =
    Ref.LedgerString.assertFromString(BaseEncoding.base16.encode(offset.toByteArray))

  implicit class ApiOffsetConverter(val offset: Offset) {
    def toApiString: Ref.LedgerString = ApiOffset.toApiString(offset)
  }

}
