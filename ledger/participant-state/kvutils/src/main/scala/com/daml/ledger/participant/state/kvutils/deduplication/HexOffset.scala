// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

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
  def firstBefore(value: Ref.HexString): Option[Ref.HexString] = {
    val (builder, found) = value.foldRight((new StringBuilder, false)) {
      case (char, (builder, found)) =>
        if (found) {
          builder.append(char) -> found
        } else {
          if (char != '0') {
            val charToAppend = if (char == 'a') '9' else (char - 1).toChar
            builder.append(charToAppend) -> true
          } else {
            builder.append('f') -> found
          }
        }
    }
    if (!found) {
      None
    } else {
      Some(Ref.HexString.assertFromString(builder.reverseInPlace().result()))
    }
  }
}
