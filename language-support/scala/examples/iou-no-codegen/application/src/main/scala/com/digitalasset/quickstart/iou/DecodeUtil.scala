// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou

import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction.Transaction

object DecodeUtil {
  def decodeCreatedEvent(transaction: Transaction): Option[CreatedEvent] =
    for {
      event <- transaction.events.headOption: Option[Event]
      created <- event.event.created: Option[CreatedEvent]
    } yield created

  def decodeArchivedEvent(transaction: Transaction): Option[ArchivedEvent] = {
    for {
      event <- transaction.events.headOption: Option[Event]
      archived <- event.event.archived: Option[ArchivedEvent]
    } yield archived
  }
}
