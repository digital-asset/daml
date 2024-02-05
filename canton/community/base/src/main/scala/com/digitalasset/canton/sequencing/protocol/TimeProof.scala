// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.OrdinaryProtocolEvent
import com.digitalasset.canton.sequencing.client.{SendAsyncClientError, SequencerClient}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{HasCryptographicEvidence, ProtoConverter}
import com.digitalasset.canton.store.SequencedEventStore.{
  IgnoredSequencedEvent,
  OrdinarySequencedEvent,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.time.v30
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import java.util.UUID
import scala.concurrent.Future

/** Wrapper for a sequenced event that has the correct properties to act as a time proof:
  *  - a deliver event with no envelopes
  *  - has a message id that suggests it was requested as a time proof (this is practically unnecessary but will act
  *     as a safeguard against future sequenced event changes)
  * @param event the signed content wrapper containing the event
  * @param deliver the time proof event itself. this must be the event content signedEvent wrapper.
  */
final case class TimeProof private (
    private val event: OrdinarySequencedEvent[Envelope[?]],
    private val deliver: Deliver[Nothing],
) extends PrettyPrinting
    with HasCryptographicEvidence {
  def timestamp: CantonTimestamp = deliver.timestamp

  def traceContext: TraceContext = event.traceContext

  override def pretty: Pretty[TimeProof.this.type] = prettyOfClass(
    unnamedParam(_.timestamp)
  )

  def toProtoV30: v30.TimeProof = v30.TimeProof(Some(event.toProtoV30))

  override def getCryptographicEvidence: ByteString = deliver.getCryptographicEvidence
}

object TimeProof {

  private def apply(
      event: OrdinarySequencedEvent[Envelope[?]],
      deliver: Deliver[Nothing],
  ): TimeProof = {
    require(
      event.signedEvent.content eq deliver,
      "Time proof event must be the content of the provided signed sequencer event",
    )
    new TimeProof(event, deliver)
  }

  def fromProtoV30(
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  )(timeProofP: v30.TimeProof): ParsingResult[TimeProof] = {
    val v30.TimeProof(eventPO) = timeProofP
    for {
      possiblyIgnoredProtocolEvent <- ProtoConverter
        .required("event", eventPO)
        .flatMap(PossiblyIgnoredSequencedEvent.fromProtoV30(protocolVersion, hashOps))
      event <- possiblyIgnoredProtocolEvent match {
        case ordinary: OrdinaryProtocolEvent => Right(ordinary)
        case _: IgnoredSequencedEvent[_] =>
          Left(ProtoDeserializationError.OtherError("Event is ignored, but must be ordinary."))
      }
      timeProof <- fromEvent(event).leftMap(ProtoDeserializationError.OtherError)
    } yield timeProof
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def fromEvent(event: OrdinarySequencedEvent[Envelope[?]]): Either[String, TimeProof] =
    for {
      deliver <- PartialFunction
        .condOpt(event.signedEvent.content) { case deliver: Deliver[_] => deliver }
        .toRight("Time Proof must be a deliver event")
      _ <- validateDeliver(deliver)
      // is now safe to cast to a `Deliver[Nothing]` as we've validated it has no envelopes
      emptyDeliver = deliver.asInstanceOf[Deliver[Nothing]]
    } yield new TimeProof(event, emptyDeliver)

  private def validateDeliver(deliver: Deliver[Envelope[?]]): Either[String, Unit] = {
    for {
      _ <- Either.cond(
        isTimeEventBatch(deliver.batch),
        (),
        "Time Proof event should have no envelopes",
      )
      _ <- Either.cond(
        deliver.messageIdO.exists(isTimeEventMessageId),
        (),
        "Time Proof event should have an expected message id",
      )
    } yield ()
  }

  /** Return a wrapped [[TimeProof]] if the given `event` has the correct properties. */
  def fromEventO(event: OrdinarySequencedEvent[Envelope[?]]): Option[TimeProof] =
    fromEvent(event).toOption

  /** Is the event a time proof */
  def isTimeProofDeliver(deliver: Deliver[Envelope[?]]): Boolean =
    validateDeliver(deliver).isRight

  /** Does the submission request look like a request to create a time event */
  def isTimeProofSubmission(submission: SubmissionRequest): Boolean =
    isTimeEventMessageId(submission.messageId) && isTimeEventBatch(submission.batch)

  /** Send placed alongside the validation logic for a time proof to help ensure it remains consistent */
  def sendRequest(
      client: SequencerClient,
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
    client.sendAsyncUnauthenticatedOrNot(
      // we intentionally ask for an empty event to be sequenced to observe the time.
      // this means we can safely share this event without mentioning other recipients.
      batch = Batch.empty(protocolVersion),
      // as we typically won't know the domain time at the point of doing this request (hence doing the request for the time...),
      // we can't pick a known good domain time for the max sequencing time.
      // if we were to guess it we may get it wrong and then in the event of no activity on the domain for our recipient,
      // we'd then never actually learn of the time.
      // so instead we just use the maximum value allowed.
      maxSequencingTime = CantonTimestamp.MaxValue,
      messageId = mkTimeProofRequestMessageId,
    )

  /** Use a constant prefix for a message which would permit the sequencer to track how many
    * time request events it is receiving.
    */
  val timeEventMessageIdPrefix = "tick-"
  private def isTimeEventMessageId(messageId: MessageId): Boolean =
    messageId.unwrap.startsWith(timeEventMessageIdPrefix)
  private def isTimeEventBatch(batch: Batch[?]): Boolean =
    batch.envelopes.isEmpty // should be entirely empty

  /** Make a unique message id for a time event submission request.
    * Currently adding a short prefix for debugging at the sequencer so floods of time requests will be observable.
    */
  @VisibleForTesting
  def mkTimeProofRequestMessageId: MessageId =
    MessageId(
      String73(s"$timeEventMessageIdPrefix${UUID.randomUUID()}")("time-proof-message-id".some)
    )
}
