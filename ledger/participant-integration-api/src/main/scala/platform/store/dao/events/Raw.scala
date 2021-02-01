// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream

import com.daml.ledger.api.v1.event.{
  ArchivedEvent => PbArchivedEvent,
  CreatedEvent => PbCreatedEvent,
  Event => PbFlatEvent,
  ExercisedEvent => PbExercisedEvent,
}
import com.daml.ledger.api.v1.transaction.{TreeEvent => PbTreeEvent}
import com.daml.logging.LoggingContext
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.serialization.Compression

import scala.collection.compat.immutable.ArraySeq
import scala.concurrent.{ExecutionContext, Future}

/** An event as it's fetched from the participant index, before
  * the deserialization the values contained therein. Allows to
  * wrap events from the database while delaying deserialization
  * so that it doesn't happen on the database thread pool.
  */
private[events] sealed trait Raw[+E] {

  /** Fill the blanks left in the raw event by running
    * the deserialization on contained values.
    *
    * @param lfValueTranslation The delegate in charge of applying deserialization
    * @param verbose If true, field names of records will be included
    */
  def applyDeserialization(
      lfValueTranslation: LfValueTranslation,
      verbose: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[E]

}

private[events] object Raw {

  /** Since created events can be both a flat event or a tree event
    * we share common code between the two variants here. What's left
    * out is wrapping the result in the proper envelope.
    */
  private[events] sealed abstract class Created[E](
      val partial: PbCreatedEvent,
      val createArgument: InputStream,
      val createArgumentCompression: Compression.Algorithm,
      val createKeyValue: Option[InputStream],
      val createKeyValueCompression: Compression.Algorithm,
  ) extends Raw[E] {
    protected def wrapInEvent(event: PbCreatedEvent): E

    final override def applyDeserialization(
        lfValueTranslation: LfValueTranslation,
        verbose: Boolean,
    )(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[E] =
      lfValueTranslation.deserialize(this, verbose).map(wrapInEvent)
  }

  private object Created {
    def apply(
        eventId: String,
        contractId: String,
        templateId: Identifier,
        createSignatories: ArraySeq[String],
        createObservers: ArraySeq[String],
        createAgreementText: Option[String],
        eventWitnesses: ArraySeq[String],
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

  sealed trait FlatEvent extends Raw[PbFlatEvent]

  object FlatEvent {

    final class Created private[Raw] (
        raw: PbCreatedEvent,
        createArgument: InputStream,
        createArgumentCompression: Compression.Algorithm,
        createKeyValue: Option[InputStream],
        createKeyValueCompression: Compression.Algorithm,
    ) extends Raw.Created[PbFlatEvent](
          raw,
          createArgument,
          createArgumentCompression,
          createKeyValue,
          createKeyValueCompression,
        )
        with FlatEvent {
      override protected def wrapInEvent(event: PbCreatedEvent): PbFlatEvent =
        PbFlatEvent(PbFlatEvent.Event.Created(event))
    }

    object Created {
      def apply(
          eventId: String,
          contractId: String,
          templateId: Identifier,
          createArgument: InputStream,
          createArgumentCompression: Option[Int],
          createSignatories: ArraySeq[String],
          createObservers: ArraySeq[String],
          createAgreementText: Option[String],
          createKeyValue: Option[InputStream],
          createKeyValueCompression: Option[Int],
          eventWitnesses: ArraySeq[String],
      ): Raw.FlatEvent.Created =
        new Raw.FlatEvent.Created(
          raw = Raw.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            createSignatories = createSignatories,
            createObservers = createObservers,
            createAgreementText = createAgreementText,
            eventWitnesses = eventWitnesses,
          ),
          createArgument = createArgument,
          createArgumentCompression = Compression.Algorithm.assertLookup(createArgumentCompression),
          createKeyValue = createKeyValue,
          createKeyValueCompression = Compression.Algorithm.assertLookup(createKeyValueCompression),
        )
    }

    /** Archived events don't actually have anything to deserialize
      */
    final class Archived private[Raw] (
        raw: PbArchivedEvent
    ) extends FlatEvent {
      override def applyDeserialization(
          lfValueTranslation: LfValueTranslation,
          verbose: Boolean,
      )(implicit
          ec: ExecutionContext,
          loggingContext: LoggingContext,
      ): Future[PbFlatEvent] =
        Future.successful(PbFlatEvent(PbFlatEvent.Event.Archived(raw)))
    }

    object Archived {
      def apply(
          eventId: String,
          contractId: String,
          templateId: Identifier,
          eventWitnesses: ArraySeq[String],
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

    final class Created(
        raw: PbCreatedEvent,
        createArgument: InputStream,
        createArgumentCompression: Compression.Algorithm,
        createKeyValue: Option[InputStream],
        createKeyValueCompression: Compression.Algorithm,
    ) extends Raw.Created[PbTreeEvent](
          raw,
          createArgument,
          createArgumentCompression,
          createKeyValue,
          createKeyValueCompression,
        )
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
          createArgumentCompression: Option[Int],
          createSignatories: ArraySeq[String],
          createObservers: ArraySeq[String],
          createAgreementText: Option[String],
          createKeyValue: Option[InputStream],
          createKeyValueCompression: Option[Int],
          eventWitnesses: ArraySeq[String],
      ): Raw.TreeEvent.Created =
        new Raw.TreeEvent.Created(
          raw = Raw.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            createSignatories = createSignatories,
            createObservers = createObservers,
            createAgreementText = createAgreementText,
            eventWitnesses = eventWitnesses,
          ),
          createArgument = createArgument,
          createArgumentCompression = Compression.Algorithm.assertLookup(createArgumentCompression),
          createKeyValue = createKeyValue,
          createKeyValueCompression = Compression.Algorithm.assertLookup(createKeyValueCompression),
        )
    }

    final class Exercised(
        val partial: PbExercisedEvent,
        val exerciseArgument: InputStream,
        val exerciseArgumentCompression: Compression.Algorithm,
        val exerciseResult: Option[InputStream],
        val exerciseResultCompression: Compression.Algorithm,
    ) extends TreeEvent {
      override def applyDeserialization(
          lfValueTranslation: LfValueTranslation,
          verbose: Boolean,
      )(implicit
          ec: ExecutionContext,
          loggingContext: LoggingContext,
      ): Future[PbTreeEvent] =
        lfValueTranslation
          .deserialize(this, verbose)
          .map(event => PbTreeEvent(PbTreeEvent.Kind.Exercised(event)))

    }

    object Exercised {
      def apply(
          eventId: String,
          contractId: String,
          templateId: Identifier,
          exerciseConsuming: Boolean,
          exerciseChoice: String,
          exerciseArgument: InputStream,
          exerciseArgumentCompression: Option[Int],
          exerciseResult: Option[InputStream],
          exerciseResultCompression: Option[Int],
          exerciseActors: ArraySeq[String],
          exerciseChildEventIds: ArraySeq[String],
          eventWitnesses: ArraySeq[String],
      ): Raw.TreeEvent.Exercised =
        new Raw.TreeEvent.Exercised(
          partial = PbExercisedEvent(
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
          exerciseArgumentCompression =
            Compression.Algorithm.assertLookup(exerciseArgumentCompression),
          exerciseResult = exerciseResult,
          exerciseResultCompression = Compression.Algorithm.assertLookup(exerciseResultCompression),
        )
    }
  }

}
