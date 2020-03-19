// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import java.util.Date

import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.ledger.api.v1.event.Event
import com.digitalasset.ledger.api.v1.transaction.{Transaction => ApiTransaction}
import com.digitalasset.ledger.api.v1.transaction_service.GetFlatTransactionResponse
import com.digitalasset.platform.ApiOffset
import com.digitalasset.platform.index.TransactionConversion
import com.google.protobuf.timestamp.Timestamp

final case class RawFlatEvent(
    eventOffset: Offset,
    transactionId: String,
    ledgerEffectiveTime: Date,
    commandId: String,
    workflowId: String,
    event: Event,
)

object RawFlatEvent {

  def aggregate(raw: List[RawFlatEvent]): Option[GetFlatTransactionResponse] = {
    raw.headOption.flatMap { first =>
      val events = TransactionConversion.removeTransient(raw.iterator.map(_.event).toVector)
      if (events.nonEmpty || first.commandId.nonEmpty)
        Some(
          GetFlatTransactionResponse(
            transaction = Some(
              ApiTransaction(
                transactionId = first.transactionId,
                commandId = first.commandId,
                effectiveAt =
                  Some(Timestamp.of(seconds = first.ledgerEffectiveTime.getTime, nanos = 0)),
                workflowId = first.workflowId,
                offset = ApiOffset.toApiString(first.eventOffset),
                events = events,
              )
            )
          )
        )
      else None

    }
  }

}
