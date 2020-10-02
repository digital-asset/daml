// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.util

import java.util.UUID

import com.daml.ledger.api.refinements.ApiTypes.{CommandId, Party}
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.{v1 => lav1}

object ClientUtil {
  def uniqueId(): String = UUID.randomUUID.toString

  def uniqueCommandId(): CommandId = CommandId(uniqueId())

  def transactionFilter(ps: Party*): TransactionFilter =
    TransactionFilter(Party.unsubst(ps).map((_, Filters.defaultInstance)).toMap)

  def boxedRecord(a: lav1.value.Record): lav1.value.Value =
    lav1.value.Value(lav1.value.Value.Sum.Record(a))
}
