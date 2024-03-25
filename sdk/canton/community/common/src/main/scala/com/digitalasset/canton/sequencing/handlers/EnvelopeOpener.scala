// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import cats.instances.either.*
import cats.syntax.either.*
import com.daml.error.{ContextualizedErrorLogger, Explanation, Resolution}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode}
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpenerError.EventDeserializationError
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, Envelope}
import com.digitalasset.canton.sequencing.{ApplicationHandler, EnvelopeBox, HandlerResult}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

/** Opener for envelopes inside an arbitrary [[EnvelopeBox]] */
class EnvelopeOpener[Box[+_ <: Envelope[_]]](protocolVersion: ProtocolVersion, hashOps: HashOps)(
    implicit Box: EnvelopeBox[Box]
) {
  def open(closed: Box[ClosedEnvelope]): ParsingResult[Box[DefaultOpenEnvelope]] =
    Box.traverse(closed)(_.openEnvelope(hashOps, protocolVersion))

  def tryOpen(closed: Box[ClosedEnvelope]): Box[DefaultOpenEnvelope] = {
    open(closed).valueOr { error =>
      throw EventDeserializationError(error, protocolVersion)
    }
  }
}

object EnvelopeOpener {

  /** Opens the envelopes inside the [[EnvelopeBox]] before handing them to the given application handler. */
  def apply[Box[+_ <: Envelope[_]]](
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  )(
      handler: ApplicationHandler[Box, DefaultOpenEnvelope]
  )(implicit
      Box: EnvelopeBox[Box],
      logger: ContextualizedErrorLogger,
  ): ApplicationHandler[Box, ClosedEnvelope] = handler.replace {
    val opener = new EnvelopeOpener[Box](protocolVersion, hashOps)
    closedEvent =>
      opener.open(closedEvent) match {
        case Right(openEnvelope) =>
          handler(openEnvelope)
        case Left(err) =>
          val alarm =
            EnvelopeOpenerError.EnvelopeOpenerDeserializationError.Error(err, protocolVersion)
          alarm.report()
          HandlerResult.done

      }
  }

}

object EnvelopeOpenerError extends SequencerErrorGroup {

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  final case class EventDeserializationError(
      error: ProtoDeserializationError,
      protocolVersion: ProtocolVersion,
      cause: Throwable = null,
  ) extends RuntimeException(
        s"Failed to deserialize event with protocol version $protocolVersion: $error",
        cause,
      )

  @Explanation("""
      |This error indicates that the sequencer client was unable to parse a message.
      |This indicates that the message has been created by a bogus or malicious sender.
      |The message will be dropped.
      |""")
  @Resolution(
    "If no other errors are reported, Canton has recovered automatically. " +
      "You should still consider to start an investigation to understand why the sender has sent an invalid message."
  )
  object EnvelopeOpenerDeserializationError extends AlarmErrorCode("EVENT_DESERIALIZATION_ERROR") {
    final case class Error(
        error: ProtoDeserializationError,
        protocolVersion: ProtocolVersion,
    ) extends Alarm(s"Failed to deserialize event with protocol version $protocolVersion: $error")
  }

}
