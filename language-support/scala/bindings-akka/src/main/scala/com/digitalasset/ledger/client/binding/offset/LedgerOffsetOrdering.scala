// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.offset

import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.{
  LEDGER_BEGIN,
  LEDGER_END
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.{Absolute, Boundary, Empty}

object LedgerOffsetOrdering {

  val ledgerBegin = LedgerOffset(Boundary(LEDGER_BEGIN))
  val ledgerEnd = LedgerOffset(Boundary(LEDGER_END))

  implicit val offsetOrdering: Ordering[LedgerOffset] = Ordering.fromLessThan { (a, b) =>
    a.value match {
      case Boundary(LEDGER_BEGIN) => true
      case Boundary(LEDGER_END) => false
      case Boundary(_) => emptyOffset(a)
      case Empty => emptyOffset(a)
      case Absolute(strA) =>
        b.value match {
          case Boundary(LEDGER_BEGIN) => false
          case Boundary(LEDGER_END) => true
          case Boundary(_) => emptyOffset(b)
          case Empty => emptyOffset(b)
          case Absolute(strB) => BigInt(strA) < BigInt(strB) // TODO this is not compatible with LS
        }
    }
  }

  private def emptyOffset(o: LedgerOffset): Boolean =
    throw new RuntimeException(s"Offset '$o' is empty")

}
