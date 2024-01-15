// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.math.BigInteger

import com.daml.lf.data.Ref

object HexOffset {

  /** Computes the first lexicographical string before [[value]], if it exists.
    * If there's no such a string (i.e., the input is 00000000...) then it returns `None`.
    * The string uses the base16 lowercase format as per [[Ref.HexString]] definition.
    *
    * Examples:
    *
    *   - for 000000000... it returns None
    *   - for 000001000 it returns Some(000000fff)
    *   - for 00000ab00 it returns Some(00000aaff)
    *   - for 00007a900 it returns Some(00007a8ff)
    */
  def previous(value: Ref.HexString): Option[Ref.HexString] = {
    val offsetInteger = new BigInteger(value, 16)
    if (offsetInteger == BigInteger.ZERO) {
      None
    } else {
      val hexString = offsetInteger.subtract(BigInteger.ONE).toString(16)
      val padding = Seq.fill(value.length - hexString.length)('0')
      Some(Ref.HexString.assertFromString(hexString.prependedAll(padding.mkString)))
    }
  }
}
