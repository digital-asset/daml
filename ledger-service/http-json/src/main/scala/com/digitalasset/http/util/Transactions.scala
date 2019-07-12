// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.transaction.Transaction

object Transactions {
  def decodeAllCreatedEvents(transaction: Transaction): Seq[CreatedEvent] =
    for {
      event <- transaction.events: Seq[Event]
      created <- event.event.created.toList: Seq[CreatedEvent]
    } yield created
}
