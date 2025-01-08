// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.utils

import com.daml.ledger.api.v2.event.Event.Event.{Archived, Created, Empty}
import com.daml.ledger.api.v2.event.{CreatedEvent, Event, ExercisedEvent}
import com.daml.ledger.api.v2.transaction.TreeEvent
import com.daml.ledger.api.v2.transaction.TreeEvent.Kind.{
  Created as TreeCreated,
  Exercised as TreeExercised,
}
import com.daml.ledger.api.v2.value.Identifier

object EventOps {

  implicit class EventOps(val event: Event) extends AnyVal {

    def nodeId: Int = event.event.nodeId

    def witnessParties: Seq[String] = event.event.witnessParties
    def updateWitnessParties(set: Seq[String]): Event =
      event.copy(event = event.event.updateWitnessParties(set))
    def modifyWitnessParties(f: Seq[String] => Seq[String]): Event =
      event.copy(event = event.event.modifyWitnessParties(f))

    def contractId: String = event.event.contractId

    def templateId: Identifier = event.event.templateId

    def isCreated: Boolean = event.event.isCreated
    def isArchived: Boolean = event.event.isArchived

  }

  implicit class EventEventOps(val event: Event.Event) extends AnyVal {

    def nodeId: Int = event match {
      case Archived(value) => value.nodeId
      case Created(value) => value.nodeId
      case Empty => throw new IllegalArgumentException("Cannot extract Event ID from Empty event.")
    }

    def witnessParties: Seq[String] = event match {
      case Archived(value) => value.witnessParties
      case Created(value) => value.witnessParties
      case Empty => Seq.empty
    }

    def updateWitnessParties(set: Seq[String]): Event.Event = event match {
      case Archived(value) => Archived(value.copy(witnessParties = set))
      case Created(value) => Created(value.copy(witnessParties = set))
      case Empty => Empty
    }

    def modifyWitnessParties(f: Seq[String] => Seq[String]): Event.Event = event match {
      case Archived(value) => Archived(value.copy(witnessParties = f(value.witnessParties)))
      case Created(value) => Created(value.copy(witnessParties = f(value.witnessParties)))
      case Empty => Empty
    }

    private def inspectTemplateId(templateId: Option[Identifier]): Identifier =
      templateId.fold(
        throw new IllegalArgumentException("Missing template id.")
      )(identity)

    def templateId: Identifier = event match {
      case Archived(value) => inspectTemplateId(value.templateId)
      case Created(value) => inspectTemplateId(value.templateId)
      case Empty =>
        throw new IllegalArgumentException("Cannot extract Template ID from Empty event.")
    }

    def contractId: String = event match {
      case Archived(value) => value.contractId
      case Created(value) => value.contractId
      case Empty =>
        throw new IllegalArgumentException("Cannot extract contractId from Empty event.")
    }

  }

  implicit final class TreeEventKindOps(val kind: TreeEvent.Kind) extends AnyVal {
    def fold[T](exercise: ExercisedEvent => T, create: CreatedEvent => T): T =
      kind match {
        case TreeExercised(value) => exercise(value)
        case TreeCreated(value) => create(value)
        case tk => throw new IllegalArgumentException(s"Unknown TreeEvent type: $tk")
      }
  }

  implicit final class TreeEventOps(val event: TreeEvent) extends AnyVal {
    def nodeId: Int = event.kind.fold(_.nodeId, _.nodeId)
    def childNodeIds: Seq[Int] = event.kind.fold(_.childNodeIds, _ => Nil)
    def filterChildNodeIds(f: Int => Boolean): TreeEvent =
      event.kind.fold(
        exercise =>
          TreeEvent(TreeExercised(exercise.copy(childNodeIds = exercise.childNodeIds.filter(f)))),
        create => TreeEvent(TreeCreated(create)),
      )
    def witnessParties: Seq[String] = event.kind.fold(_.witnessParties, _.witnessParties)
    def modifyWitnessParties(f: Seq[String] => Seq[String]): TreeEvent =
      event.kind.fold(
        exercise =>
          TreeEvent(TreeExercised(exercise.copy(witnessParties = f(exercise.witnessParties)))),
        create => TreeEvent(TreeCreated(create.copy(witnessParties = f(create.witnessParties)))),
      )
    def templateId: Option[Identifier] = event.kind.fold(_.templateId, _.templateId)
  }

}
