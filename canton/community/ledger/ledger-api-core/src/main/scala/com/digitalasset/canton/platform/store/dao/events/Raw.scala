// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v1.event.{
  ArchivedEvent as PbArchivedEvent,
  CreatedEvent as PbCreatedEvent,
  Event as PbFlatEvent,
  ExercisedEvent as PbExercisedEvent,
}
import com.daml.ledger.api.v1.transaction.TreeEvent as PbTreeEvent
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.Identifier
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.serialization.Compression
import com.digitalasset.canton.util.LfEngineToApi

import scala.collection.immutable.ArraySeq
import scala.concurrent.{ExecutionContext, Future}

/** An event as it's fetched from the participant index, before
  * the deserialization the values contained therein. Allows to
  * wrap events from the database while delaying deserialization
  * so that it doesn't happen on the database thread pool.
  */
sealed trait Raw[+E] {

  /** Fill the blanks left in the raw event by running
    * the deserialization on contained values.
    *
    * @param lfValueTranslation The delegate in charge of applying deserialization
    * @param eventProjectionProperties The properties of how contract arguments and interface views for
    *                                  the event are projected and merged
    */
  def applyDeserialization(
      lfValueTranslation: LfValueTranslation,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[E]

  def witnesses: Seq[String]

}

object Raw {

  /** Since created events can be both a flat event or a tree event
    * we share common code between the two variants here. What's left
    * out is wrapping the result in the proper envelope.
    */
  sealed abstract class Created[E](
      val partial: PbCreatedEvent,
      val createArgument: Array[Byte],
      val createArgumentCompression: Compression.Algorithm,
      val createKeyValue: Option[Array[Byte]],
      val createKeyMaintainers: Array[String],
      val createKeyValueCompression: Compression.Algorithm,
      val driverMetadata: Option[Array[Byte]],
  ) extends Raw[E] {
    protected def wrapInEvent(event: PbCreatedEvent): E

    final override def applyDeserialization(
        lfValueTranslation: LfValueTranslation,
        eventProjectionProperties: EventProjectionProperties,
    )(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContextWithTrace,
    ): Future[E] =
      deserializeCreateEvent(lfValueTranslation, eventProjectionProperties).map(wrapInEvent)

    def deserializeCreateEvent(
        lfValueTranslation: LfValueTranslation,
        eventProjectionProperties: EventProjectionProperties,
    )(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContextWithTrace,
    ): Future[PbCreatedEvent] =
      lfValueTranslation.deserialize(this, eventProjectionProperties)

  }

  object Created {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    def apply(
        eventId: String,
        contractId: String,
        templateId: Identifier,
        createSignatories: ArraySeq[String],
        createObservers: ArraySeq[String],
        createAgreementText: Option[String],
        eventWitnesses: ArraySeq[String],
        createKeyHash: Option[Hash],
        ledgerEffectiveTime: Timestamp,
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
        createdAt = Some(TimestampConversion.fromLf(ledgerEffectiveTime)),
      )
  }

  sealed trait FlatEvent extends Raw[PbFlatEvent]

  object FlatEvent {

    final class Created private[Raw] (
        raw: PbCreatedEvent,
        createArgument: Array[Byte],
        createArgumentCompression: Compression.Algorithm,
        createKeyValue: Option[Array[Byte]],
        createKeyMaintainers: Array[String],
        createKeyValueCompression: Compression.Algorithm,
        driverMetadata: Option[Array[Byte]],
    ) extends Raw.Created[PbFlatEvent](
          raw,
          createArgument,
          createArgumentCompression,
          createKeyValue,
          createKeyMaintainers,
          createKeyValueCompression,
          driverMetadata,
        )
        with FlatEvent {
      override protected def wrapInEvent(event: PbCreatedEvent): PbFlatEvent =
        PbFlatEvent(PbFlatEvent.Event.Created(event))

      override def witnesses: Seq[String] = raw.witnessParties

      def stakeholders: Set[String] = raw.signatories.toSet ++ raw.observers
    }

    object Created {
      def apply(
          eventId: String,
          contractId: String,
          templateId: Identifier,
          createArgument: Array[Byte],
          createArgumentCompression: Option[Int],
          createSignatories: ArraySeq[String],
          createObservers: ArraySeq[String],
          createAgreementText: Option[String],
          createKeyValue: Option[Array[Byte]],
          createKeyHash: Option[Hash],
          createKeyValueCompression: Option[Int],
          createKeyMaintainers: Array[String],
          ledgerEffectiveTime: Timestamp,
          eventWitnesses: ArraySeq[String],
          driverMetadata: Option[Array[Byte]],
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
            createKeyHash = createKeyHash,
            ledgerEffectiveTime = ledgerEffectiveTime,
          ),
          createArgument = createArgument,
          createArgumentCompression = Compression.Algorithm.assertLookup(createArgumentCompression),
          createKeyValue = createKeyValue,
          createKeyMaintainers = createKeyMaintainers,
          createKeyValueCompression = Compression.Algorithm.assertLookup(createKeyValueCompression),
          driverMetadata = driverMetadata,
        )
    }

    /** Archived events don't actually have anything to deserialize
      */
    final class Archived private[Raw] (
        raw: PbArchivedEvent
    ) extends FlatEvent {
      override def applyDeserialization(
          lfValueTranslation: LfValueTranslation,
          eventProjectionProperties: EventProjectionProperties,
      )(implicit
          ec: ExecutionContext,
          loggingContext: LoggingContextWithTrace,
      ): Future[PbFlatEvent] =
        Future.successful(PbFlatEvent(PbFlatEvent.Event.Archived(raw)))

      def deserializedArchivedEvent(): PbArchivedEvent = raw

      override def witnesses: Seq[String] = raw.witnessParties
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
        createArgument: Array[Byte],
        createArgumentCompression: Compression.Algorithm,
        createKeyValue: Option[Array[Byte]],
        createKeyMaintainers: Array[String],
        createKeyValueCompression: Compression.Algorithm,
        driverMetadata: Option[Array[Byte]],
    ) extends Raw.Created[PbTreeEvent](
          raw,
          createArgument,
          createArgumentCompression,
          createKeyValue,
          createKeyMaintainers,
          createKeyValueCompression,
          driverMetadata,
        )
        with TreeEvent {
      override protected def wrapInEvent(event: PbCreatedEvent): PbTreeEvent =
        PbTreeEvent(PbTreeEvent.Kind.Created(event))

      override def witnesses: Seq[String] = raw.witnessParties
    }

    object Created {
      def apply(
          eventId: String,
          contractId: String,
          templateId: Identifier,
          createArgument: Array[Byte],
          createArgumentCompression: Option[Int],
          createSignatories: ArraySeq[String],
          createObservers: ArraySeq[String],
          createAgreementText: Option[String],
          createKeyValue: Option[Array[Byte]],
          createKeyHash: Option[Hash],
          createKeyValueCompression: Option[Int],
          createKeyMaintainers: Array[String],
          ledgerEffectiveTime: Timestamp,
          eventWitnesses: ArraySeq[String],
          driverMetadata: Option[Array[Byte]],
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
            createKeyHash = createKeyHash,
            ledgerEffectiveTime = ledgerEffectiveTime,
          ),
          createArgument = createArgument,
          createArgumentCompression = Compression.Algorithm.assertLookup(createArgumentCompression),
          createKeyValue = createKeyValue,
          createKeyMaintainers = createKeyMaintainers,
          createKeyValueCompression = Compression.Algorithm.assertLookup(createKeyValueCompression),
          driverMetadata = driverMetadata,
        )
    }

    final class Exercised(
        val partial: PbExercisedEvent,
        val exerciseArgument: Array[Byte],
        val exerciseArgumentCompression: Compression.Algorithm,
        val exerciseResult: Option[Array[Byte]],
        val exerciseResultCompression: Compression.Algorithm,
    ) extends TreeEvent {
      override def applyDeserialization(
          lfValueTranslation: LfValueTranslation,
          eventProjectionProperties: EventProjectionProperties,
      )(implicit
          ec: ExecutionContext,
          loggingContext: LoggingContextWithTrace,
      ): Future[PbTreeEvent] =
        lfValueTranslation
          .deserialize(this, eventProjectionProperties.verbose)
          .map(event => PbTreeEvent(PbTreeEvent.Kind.Exercised(event)))

      override def witnesses: Seq[String] = partial.witnessParties
    }

    object Exercised {
      @SuppressWarnings(Array("org.wartremover.warts.Null"))
      def apply(
          eventId: String,
          contractId: String,
          templateId: Identifier,
          interfaceId: Option[Identifier],
          exerciseConsuming: Boolean,
          exerciseChoice: String,
          exerciseArgument: Array[Byte],
          exerciseArgumentCompression: Option[Int],
          exerciseResult: Option[Array[Byte]],
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
            interfaceId = interfaceId.map(LfEngineToApi.toApiIdentifier),
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
