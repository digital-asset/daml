// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import cats.instances.either.*
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpener.EventDeserializationError
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, Envelope}
import com.digitalasset.canton.sequencing.{ApplicationHandler, EnvelopeBox}
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
      // TODO(i12902) find all usages of this method and change them to use open.
      //  Throwing an exception here will likely cause availability issues.
      throw EventDeserializationError(error, protocolVersion)
    }
  }
}

object EnvelopeOpener {

  /** Opens the envelopes inside the [[EnvelopeBox]] before handing them to the given application handler. */
  def apply[Box[+_ <: Envelope[_]]](protocolVersion: ProtocolVersion, hashOps: HashOps)(
      handler: ApplicationHandler[Box, DefaultOpenEnvelope]
  )(implicit Box: EnvelopeBox[Box]): ApplicationHandler[Box, ClosedEnvelope] = handler.replace {
    val opener = new EnvelopeOpener[Box](protocolVersion, hashOps)

    closedEvent => handler(opener.tryOpen(closedEvent))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  final case class EventDeserializationError(
      error: ProtoDeserializationError,
      protocolVersion: ProtocolVersion,
      cause: Throwable = null,
  ) extends RuntimeException(
        s"Failed to deserialize event with protocol version $protocolVersion: $error",
        cause,
      )
}
