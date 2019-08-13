// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.{
  LEDGER_BEGIN,
  LEDGER_END
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.Boundary
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.v1.value.Identifier

object TransactionFilters {

  def empty = TransactionFilter()

  def allForParties(parties: String*) =
    TransactionFilter(parties.map(_ -> Filters()).toMap)

  def templatesByParty(templatesByParty: (String, Seq[Identifier])*) =
    TransactionFilter(
      templatesByParty.toMap.mapValues(templateIds => Filters(Some(InclusiveFilters(templateIds)))))

}

object LedgerOffsets {
  val LedgerBegin = LedgerOffset(Boundary(LEDGER_BEGIN))
  val LedgerEnd = LedgerOffset(Boundary(LEDGER_END))
}

object TestParties {
  val Alice = "Alice"
  val Bob = "Bob"
  val Charlie = "Charlie"
  val Dan = "Dan"
  val Eve = "Eve"
  val Frank = "Frank"
  val Grace = "Grace"
  val Heidi = "Heidi"
  val Ivan = "Ivan"

  val AllParties = List(Alice, Bob, Charlie, Dan, Eve, Frank, Grace, Heidi, Ivan)
}
