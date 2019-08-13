// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.quickstart.iou

import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.transaction.Transaction

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
