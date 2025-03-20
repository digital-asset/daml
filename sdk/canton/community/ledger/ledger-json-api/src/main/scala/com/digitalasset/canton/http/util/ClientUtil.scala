// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.ledger.api.v2 as lav2
import com.daml.ledger.api.v2.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.{CommandId, Party}

import java.util.UUID

object ClientUtil {
  def uniqueId(): String = UUID.randomUUID.toString

  def uniqueCommandId(): CommandId = CommandId(uniqueId())

  def transactionFilter(ps: Party*): TransactionFilter =
    TransactionFilter(Party.unsubst(ps).map((_, Filters.defaultInstance)).toMap, None)

  import com.digitalasset.canton.fetchcontracts.util.ClientUtil as FC

  def boxedRecord(a: lav2.value.Record): lav2.value.Value =
    FC.boxedRecord(a)
}
