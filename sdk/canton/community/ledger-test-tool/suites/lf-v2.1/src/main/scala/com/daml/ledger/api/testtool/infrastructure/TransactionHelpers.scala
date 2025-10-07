// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v2.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.daml.ledger.api.v2.transaction.Transaction

object TransactionHelpers {
  def archivedEvents(transaction: Transaction): Vector[ArchivedEvent] =
    events(transaction).flatMap(_.event.archived.toList).toVector

  def createdEvents(transaction: Transaction): Vector[CreatedEvent] =
    events(transaction).flatMap(_.event.created.toList).toVector

  def exercisedEvents(transaction: Transaction): Vector[ExercisedEvent] =
    events(transaction).flatMap(_.event.exercised.toList).toVector

  private def events(transaction: Transaction): Iterator[Event] =
    transaction.events.iterator
}
