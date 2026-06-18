// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.base.error.{Alarm, AlarmErrorCode, Explanation, Resolution}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.sequencing.{
  ApplicationHandler,
  OrdinaryApplicationHandler,
  OrdinaryEnvelopeBox,
}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.version.ProtocolVersion

object EnvelopeOpener {

  /** Opens the envelopes inside the [[EnvelopeBox]] before handing them to the given application
    * handler. Envelopes that cannot be opened are discarded, possibly resulting in an event with an
    * empty batch.
    */
  def apply(
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  )(
      handler: OrdinaryApplicationHandler[DefaultOpenEnvelope]
  )(implicit
      logger: ErrorLoggingContext
  ): ApplicationHandler[OrdinaryEnvelopeBox, ClosedEnvelope] = handler.replace { tracedEvents =>
    val openedEvents = tracedEvents.map { closedEvents =>
      closedEvents.map { event =>
        val openedEvent = OrdinarySequencedEvent.openEnvelopes(event)(protocolVersion, hashOps)
        openedEvent.openingErrors.foreach { error =>
          EnvelopeOpenerError.EnvelopeOpenerDeserializationError
            .Error(error, protocolVersion)
            .report()
        }
        openedEvent.event
      }
    }
    handler(openedEvents)
  }
}

object EnvelopeOpenerError extends SequencerErrorGroup {

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
