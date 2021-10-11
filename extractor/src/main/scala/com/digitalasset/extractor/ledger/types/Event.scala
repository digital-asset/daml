// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.ledger.types

import com.daml.lf.value.{Value => V}
import com.daml.ledger.api.{v1 => api}
import Identifier._
import LedgerValue._
import api.event

import scalaz._
import Scalaz._

sealed trait Event extends Serializable with Product

final case class CreatedEvent(
    eventId: String,
    contractId: String,
    templateId: Identifier,
    createArguments: V.ValueRecord,
    stakeholders: Set[String],
) extends Event

final case class ExercisedEvent(
    eventId: String,
    contractId: String,
    templateId: Identifier,
    choice: String,
    choiceArgument: LedgerValue,
    actingParties: Set[String],
    consuming: Boolean,
    witnessParties: Set[String],
    childEventIds: Seq[String],
) extends Event

object Event {
  private val createdTemplateIdLens =
    ReqFieldLens.create[api.event.CreatedEvent, api.value.Identifier](Symbol("templateId"))
  private val createdArgumentsLens =
    ReqFieldLens.create[api.event.CreatedEvent, api.value.Record](Symbol("createArguments"))
  private val exercisedTemplateIdLens =
    ReqFieldLens.create[api.event.ExercisedEvent, api.value.Identifier](Symbol("templateId"))
  private val exercisedChoiceArgLens =
    ReqFieldLens.create[api.event.ExercisedEvent, api.value.Value](Symbol("choiceArgument"))

  final implicit class ApiEventOps(val apiEvent: api.event.Event.Event) extends AnyVal {
    def convert: String \/ Event = apiEvent match {
      case api.event.Event.Event.Archived(event) =>
        s"Unexpected `Archived` event: $event. Only `Created` events are expected.".left
      case api.event.Event.Event.Created(event) => event.convert.widen
      case api.event.Event.Event.Empty => "Unexpected `Empty` event.".left
    }
  }

  final implicit class CreatedEventOps(val apiEvent: event.CreatedEvent) extends AnyVal {
    def convert: String \/ CreatedEvent = {
      for {
        apiTemplateId <- createdTemplateIdLens(apiEvent)
        templateId = apiTemplateId.convert
        apiRecord <- createdArgumentsLens(apiEvent)
        createArguments <- apiRecord.convert
      } yield CreatedEvent(
        apiEvent.eventId,
        apiEvent.contractId,
        templateId,
        createArguments,
        (apiEvent.observers ++ apiEvent.signatories).toSet,
      )
    }
  }

  final implicit class ExercisedEventOps(val apiEvent: event.ExercisedEvent) extends AnyVal {
    def convert: String \/ ExercisedEvent = {
      for {
        apiTemplateId <- exercisedTemplateIdLens(apiEvent)
        templateId = apiTemplateId.convert
        apiChoiceArg <- exercisedChoiceArgLens(apiEvent)
        choiceArg <- apiChoiceArg.convert
      } yield ExercisedEvent(
        apiEvent.eventId,
        apiEvent.contractId,
        templateId,
        apiEvent.choice,
        choiceArg,
        apiEvent.actingParties.toSet,
        apiEvent.consuming,
        apiEvent.witnessParties.toSet,
        apiEvent.childEventIds,
      )
    }
  }
}
