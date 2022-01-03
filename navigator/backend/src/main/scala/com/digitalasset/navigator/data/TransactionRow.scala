// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.data

import java.time.Instant

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.model.{Event, Transaction}

import scalaz.syntax.tag._

final case class TransactionRow(
    autoInc: Option[Int],
    id: String,
    commandId: Option[String],
    effectiveAt: String,
    offset: String,
) {

  def toTransaction(events: List[Event]): Transaction = {
    Transaction(
      ApiTypes.TransactionId(id),
      commandId.map(ApiTypes.CommandId(_)),
      Instant.parse(effectiveAt),
      offset,
      events,
    )
  }
}

object TransactionRow {

  def fromTransaction(tx: Transaction): TransactionRow = {
    TransactionRow(
      None,
      tx.id.unwrap,
      tx.commandId.map(_.unwrap),
      tx.effectiveAt.toString,
      tx.offset,
    )
  }
}
