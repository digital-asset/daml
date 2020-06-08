package com.daml.platform

import com.daml.lf.types.Ledger

package object events {

  type TransactionIdWithIndex = Ledger.EventId
  val TransactionIdWithIndex: Ledger.EventId.type = Ledger.EventId

}
