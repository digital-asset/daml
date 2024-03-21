// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import java.util.UUID

import com.digitalasset.canton.ledger.api.refinements.ApiTypes.{CommandId, Party}
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.{v1 => lav1}

object ClientUtil {
  def uniqueId(): String = UUID.randomUUID.toString

  def uniqueCommandId(): CommandId = CommandId(uniqueId())

  def transactionFilter(ps: Party*): TransactionFilter =
    TransactionFilter(Party.unsubst(ps).map((_, Filters.defaultInstance)).toMap)

  import com.digitalasset.canton.fetchcontracts.util.ClientUtil as FC

  def boxedRecord(a: lav1.value.Record): lav1.value.Value =
    FC.boxedRecord(a)
}
