// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.api.v1.event

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain.{ContractId, EventId}
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created, Empty}
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event, ExercisedEvent}
import com.digitalasset.ledger.api.v1.transaction.TreeEvent
import com.digitalasset.ledger.api.v1.transaction.TreeEvent.Kind.{
  Created => TreeCreated,
  Exercised => TreeExercised
}
import scalaz.Tag
import scalaz.syntax.tag._

object EventOps {

  private val eventIndexFromIdRegex = """.*[-:](\d+)$""".r

  implicit class EventOps(val event: Event) extends AnyVal {

    def eventIndex: Int = event.event.eventIndex

    def eventId: EventId = event.event.eventId

    def witnesses: Seq[String] = event.event.witnesses

    def contractId: ContractId = event.event.contractId

    def templateId: String = event.event.templateId

    def withWitnesses(witnesses: Seq[String]): Event = Event(event.event.withWitnesses(witnesses))

  }

  implicit class EventEventOps(val event: Event.Event) extends AnyVal {

    def eventIndex: Int = getEventIndex(event.event.eventId.unwrap)

    def eventId: EventId = event match {
      case Archived(value) => EventId(Ref.LedgerString.assertFromString(value.eventId))
      case Created(value) => EventId(Ref.LedgerString.assertFromString(value.eventId))
      case Empty => throw new IllegalArgumentException("Cannot extract Event ID from Empty event.")
    }

    def witnesses: Seq[String] = event match {
      case c: Created => c.value.witnessParties
      case a: Archived => a.value.witnessParties
      case Empty => Seq.empty
    }

    def templateId: String = event match {
      case c: Created => c.templateId
      case a: Archived => a.templateId
      case Empty =>
        throw new IllegalArgumentException("Cannot extract Template ID from Empty event.")
    }

    def contractId: ContractId = event match {
      case Archived(value) => ContractId(value.contractId)
      case Created(value) => ContractId(value.contractId)
      case Empty =>
        throw new IllegalArgumentException("Cannot extract contractId from Empty event.")
    }

    def withWitnesses(witnesses: Seq[String]): Event.Event = event match {
      case c: Created => Created(c.value.copy(witnessParties = witnesses))
      case a: Archived => Archived(a.value.copy(witnessParties = witnesses))
      case Empty => Empty
    }

  }

  def getEventIndex(eventId: String): Int = {
    eventIndexFromIdRegex
      .findFirstMatchIn(eventId)
      .fold {
        throw new IllegalArgumentException(
          s"Event ID $eventId does not match regex $eventIndexFromIdRegex")
      } {
        _.group(1).toInt
      }
  }

  implicit class TreeEventKindOps(kind: TreeEvent.Kind) {
    def fold[T](exerciseHandler: ExercisedEvent => T, createHandler: CreatedEvent => T) =
      kind match {
        case e: TreeExercised => exerciseHandler(e.value)
        case c: TreeCreated => createHandler(c.value)
        case tk => throw new IllegalArgumentException(s"Unknown TreeEvent type: $tk")
      }
  }

  implicit class TreeEventOps(val event: TreeEvent) {
    def eventId: EventId =
      event.kind.fold(
        e => EventId(Ref.LedgerString.assertFromString(e.eventId)),
        c => EventId(Ref.LedgerString.assertFromString(c.eventId)))
    def children: Seq[EventId] =
      event.kind
        .fold(e => Tag.subst(e.childEventIds.map(Ref.LedgerString.assertFromString)), _ => Nil)
    def witnessParties: Seq[String] = event.kind.fold(_.witnessParties, _.witnessParties)
  }

}
