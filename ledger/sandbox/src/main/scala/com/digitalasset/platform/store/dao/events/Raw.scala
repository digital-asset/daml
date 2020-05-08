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

sealed trait Raw[+E] {
  def applyDeserialization(verbose: Boolean): E
}

object Raw {

  private def toApiValue(
      value: InputStream,
      verbose: Boolean,
      failureContext: => String,
  ): ApiValue =
    LfEngineToApi.assertOrRuntimeEx(
      failureContext = failureContext,
      LfEngineToApi
        .lfVersionedValueToApiValue(
          verbose = verbose,
          value = ValueSerializer.deserializeValue(value),
        ),
    )

  private def toApiRecord(
      value: InputStream,
      verbose: Boolean,
      failureContext: => String,
  ): ApiRecord =
    LfEngineToApi.assertOrRuntimeEx(
      failureContext = failureContext,
      LfEngineToApi
        .lfVersionedValueToApiRecord(
          verbose = verbose,
          recordValue = ValueSerializer.deserializeValue(value),
        ),
    )

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
                failureContext = s"attempting to deserialize persisted key to value",
            )
          ),
          createArguments = Some(
            toApiRecord(
              value = createArgument,
              verbose = verbose,
              failureContext = s"attempting to deserialize persisted create argument to record",
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
              choiceArgument =
                Some(
                  toApiValue(
                    value = exerciseArgument,
                    verbose = verbose,
                    failureContext =
                      s"attempting to deserialize persisted exercise argument to value",
                  )
                ),
              exerciseResult =
                exerciseResult.map(
                  result =>
                    toApiValue(
                      value = result,
                      verbose = verbose,
                      failureContext =
                        s"attempting to deserialize persisted exercise result to value",
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
