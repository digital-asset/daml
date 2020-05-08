// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream

import com.daml.ledger.api.v1.event.{
  ArchivedEvent => PbArchivedEvent,
  CreatedEvent => PbCreatedEvent,
  Event => PbFlatEvent,
  ExercisedEvent => PbExercisedEvent
}
import com.daml.ledger.api.v1.transaction.{TreeEvent => PbTreeEvent}
import com.daml.ledger.api.v1.value.{Record => ApiRecord, Value => ApiValue}
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.serialization.ValueSerializer

/**
  * An event as it's fetched from the participant index, before
  * the deserialization the values contained therein. Allows to
  * wrap events from the database while delaying deserialization
  * so that it doesn't happen on the database thread pool.
  */
sealed trait Raw[+E] {

  /**
    * Fill the blanks left in the raw event by running
    * the deserialization on contained values.
    *
    * @param verbose If true, field names of records will be included
    */
  def applyDeserialization(verbose: Boolean): E

}

object Raw {

  private def toApiValue(
      value: InputStream,
      verbose: Boolean,
      attribute: => String,
  ): ApiValue =
    LfEngineToApi.assertOrRuntimeEx(
      failureContext = s"attempting to deserialize persisted $attribute to value",
      LfEngineToApi
        .lfVersionedValueToApiValue(
          verbose = verbose,
          value = ValueSerializer.deserializeValue(value),
        ),
    )

  private def toApiRecord(
      value: InputStream,
      verbose: Boolean,
      attribute: => String,
  ): ApiRecord =
    LfEngineToApi.assertOrRuntimeEx(
      failureContext = s"attempting to deserialize persisted $attribute to record",
      LfEngineToApi
        .lfVersionedValueToApiRecord(
          verbose = verbose,
          recordValue = ValueSerializer.deserializeValue(value),
        ),
    )

  /**
    * Since created events can be both a flat event or a tree event
    * we share common code between the two variants here. What's left
    * out is wrapping the result in the proper envelope.
    */
  private[events] sealed abstract class Created[E] private[Raw] (
      raw: PbCreatedEvent,
      createArgument: InputStream,
      createKeyValue: Option[InputStream],
  ) extends Raw[E] {
    protected def wrapInEvent(event: PbCreatedEvent): E
    final override def applyDeserialization(verbose: Boolean): E =
      wrapInEvent(
        raw.copy(
          contractKey = createKeyValue.map(
            key =>
              toApiValue(
                value = key,
                verbose = verbose,
                attribute = "create key",
            )
          ),
          createArguments = Some(
            toApiRecord(
              value = createArgument,
              verbose = verbose,
              attribute = "create argument",
            )
          ),
        )
      )
  }

  private object Created {
    def apply(
        eventId: String,
        contractId: String,
        templateId: Identifier,
        createSignatories: Array[String],
        createObservers: Array[String],
        createAgreementText: Option[String],
        eventWitnesses: Array[String],
    ): PbCreatedEvent =
      PbCreatedEvent(
        eventId = eventId,
        contractId = contractId,
        templateId = Some(LfEngineToApi.toApiIdentifier(templateId)),
        contractKey = null,
        createArguments = null,
        witnessParties = eventWitnesses,
        signatories = createSignatories,
        observers = createObservers,
        agreementText = createAgreementText.orElse(Some("")),
      )
  }

  /**
    * Defines a couple of methods on flat events to allow the events
    * to be manipulated before running deserialization (specifically,
    * removing transient contracts).
    */
  sealed trait FlatEvent extends Raw[PbFlatEvent] {
    def isCreated: Boolean
    def contractId: String
  }

  object FlatEvent {

    final class Created private[Raw] (
        raw: PbCreatedEvent,
        createArgument: InputStream,
        createKeyValue: Option[InputStream],
    ) extends Raw.Created[PbFlatEvent](raw, createArgument, createKeyValue)
        with FlatEvent {
      override val isCreated: Boolean = true
      override val contractId: String = raw.contractId
      override protected def wrapInEvent(event: PbCreatedEvent): PbFlatEvent =
        PbFlatEvent(PbFlatEvent.Event.Created(event))
    }

    object Created {
      def apply(
          eventId: String,
          contractId: String,
          templateId: Identifier,
          createArgument: InputStream,
          createSignatories: Array[String],
          createObservers: Array[String],
          createAgreementText: Option[String],
          createKeyValue: Option[InputStream],
          eventWitnesses: Array[String],
      ): Raw.FlatEvent.Created =
        new Raw.FlatEvent.Created(
          raw = Raw.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            createSignatories = createSignatories,
            createObservers = createObservers,
            createAgreementText = createAgreementText,
            eventWitnesses = eventWitnesses
          ),
          createArgument = createArgument,
          createKeyValue = createKeyValue,
        )
    }

    /**
      * Archived events don't actually have anything to deserialize
      */
    final class Archived private[Raw] (
        raw: PbArchivedEvent,
    ) extends FlatEvent {
      override val isCreated: Boolean = false
      override val contractId: String = raw.contractId
      override def applyDeserialization(verbose: Boolean): PbFlatEvent =
        PbFlatEvent(PbFlatEvent.Event.Archived(raw))
    }

    object Archived {
      def apply(
          eventId: String,
          contractId: String,
          templateId: Identifier,
          eventWitnesses: Array[String],
      ): Raw.FlatEvent.Archived =
        new Raw.FlatEvent.Archived(
          raw = PbArchivedEvent(
            eventId = eventId,
            contractId = contractId,
            templateId = Some(LfEngineToApi.toApiIdentifier(templateId)),
            witnessParties = eventWitnesses,
          )
        )

    }

  }

  sealed trait TreeEvent extends Raw[PbTreeEvent]

  object TreeEvent {

    final class Created private[Raw] (
        raw: PbCreatedEvent,
        createArgument: InputStream,
        createKeyValue: Option[InputStream],
    ) extends Raw.Created[PbTreeEvent](raw, createArgument, createKeyValue)
        with TreeEvent {
      override protected def wrapInEvent(event: PbCreatedEvent): PbTreeEvent =
        PbTreeEvent(PbTreeEvent.Kind.Created(event))
    }

    object Created {
      def apply(
          eventId: String,
          contractId: String,
          templateId: Identifier,
          createArgument: InputStream,
          createSignatories: Array[String],
          createObservers: Array[String],
          createAgreementText: Option[String],
          createKeyValue: Option[InputStream],
          eventWitnesses: Array[String],
      ): Raw.TreeEvent.Created =
        new Raw.TreeEvent.Created(
          raw = Raw.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            createSignatories = createSignatories,
            createObservers = createObservers,
            createAgreementText = createAgreementText,
            eventWitnesses = eventWitnesses
          ),
          createArgument = createArgument,
          createKeyValue = createKeyValue,
        )
    }

    final class Exercised(
        base: PbExercisedEvent,
        exerciseArgument: InputStream,
        exerciseResult: Option[InputStream],
    ) extends TreeEvent {
      override def applyDeserialization(verbose: Boolean): PbTreeEvent =
        PbTreeEvent(
          PbTreeEvent.Kind.Exercised(
            base.copy(
              choiceArgument = Some(
                toApiValue(
                  value = exerciseArgument,
                  verbose = verbose,
                  attribute = "exercise argument",
                )
              ),
              exerciseResult = exerciseResult.map(
                result =>
                  toApiValue(
                    value = result,
                    verbose = verbose,
                    attribute = "exercise result",
                )
              ),
            )
          ))
    }

    object Exercised {
      def apply(
          eventId: String,
          contractId: String,
          templateId: Identifier,
          exerciseConsuming: Boolean,
          exerciseChoice: String,
          exerciseArgument: InputStream,
          exerciseResult: Option[InputStream],
          exerciseActors: Array[String],
          exerciseChildEventIds: Array[String],
          eventWitnesses: Array[String],
      ): Raw.TreeEvent.Exercised =
        new Raw.TreeEvent.Exercised(
          base = PbExercisedEvent(
            eventId = eventId,
            contractId = contractId,
            templateId = Some(LfEngineToApi.toApiIdentifier(templateId)),
            choice = exerciseChoice,
            choiceArgument = null,
            actingParties = exerciseActors,
            consuming = exerciseConsuming,
            witnessParties = eventWitnesses,
            childEventIds = exerciseChildEventIds,
            exerciseResult = null,
          ),
          exerciseArgument = exerciseArgument,
          exerciseResult = exerciseResult,
        )
    }
  }

}
