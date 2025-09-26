// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.utils

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.event.Event.Event.{Archived, Created, Empty, Exercised}
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
      case Exercised(value) => value.nodeId
      case Empty => throw new IllegalArgumentException("Cannot extract Event ID from Empty event.")
    }

    def witnessParties: Seq[String] = event match {
      case Archived(value) => value.witnessParties
      case Created(value) => value.witnessParties
      case Exercised(value) => value.witnessParties
      case Empty => Seq.empty
    }

    def updateWitnessParties(set: Seq[String]): Event.Event = event match {
      case Archived(value) => Archived(value.copy(witnessParties = set))
      case Created(value) => Created(value.copy(witnessParties = set))
      case Exercised(value) => Exercised(value.copy(witnessParties = set))
      case Empty => Empty
    }

    def modifyWitnessParties(f: Seq[String] => Seq[String]): Event.Event = event match {
      case Archived(value) => Archived(value.copy(witnessParties = f(value.witnessParties)))
      case Created(value) => Created(value.copy(witnessParties = f(value.witnessParties)))
      case Exercised(value) => Exercised(value.copy(witnessParties = f(value.witnessParties)))
      case Empty => Empty
    }

    private def inspectTemplateId(templateId: Option[Identifier]): Identifier =
      templateId.fold(
        throw new IllegalArgumentException("Missing template id.")
      )(identity)

    def templateId: Identifier = event match {
      case Archived(value) => inspectTemplateId(value.templateId)
      case Created(value) => inspectTemplateId(value.templateId)
      case Exercised(value) => inspectTemplateId(value.templateId)
      case Empty =>
        throw new IllegalArgumentException("Cannot extract Template ID from Empty event.")
    }

    def contractId: String = event match {
      case Archived(value) => value.contractId
      case Created(value) => value.contractId
      case Exercised(value) => value.contractId
      case Empty =>
        throw new IllegalArgumentException("Cannot extract contractId from Empty event.")
    }

  }
}
