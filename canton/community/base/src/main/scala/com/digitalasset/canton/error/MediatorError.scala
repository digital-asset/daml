// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.*
import com.digitalasset.canton.error.CantonErrorGroups.MediatorErrorGroup
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import org.slf4j.event.Level

sealed trait MediatorError extends Product with Serializable with PrettyPrinting

object MediatorError extends MediatorErrorGroup {

  @Explanation(
    """This rejection indicates that the transaction has been rejected by the mediator as it didn't receive enough confirmations within the participant response timeout.
      The field "unresponsiveParties" in the error info contains the comma-separated list of parties that failed to send a response within the participant response timeout. This field is only present since protocol version 6"""
  )
  @Resolution(
    "Check that all involved participants are available and not overloaded."
  )
  object Timeout
      extends ErrorCode(
        id = "MEDIATOR_SAYS_TX_TIMED_OUT",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Reject(
        override val cause: String = Reject.defaultCause,
        unresponsiveParties: String = "",
    ) extends BaseCantonError.Impl(cause)
        with MediatorError {
      override def pretty: Pretty[Reject] = prettyOfClass(
        param("code", _.code.id.unquoted),
        param("cause", _.cause.unquoted),
        param(
          "unresponsive parties",
          _.unresponsiveParties.unquoted,
          _.unresponsiveParties.nonEmpty,
        ),
      )
    }
    object Reject {
      val defaultCause: String =
        "Rejected transaction as the mediator did not receive sufficient confirmations within the expected timeframe."
    }
  }

  @Explanation(
    """The mediator has received an invalid message (request or response).
      |The message will be discarded. As a consequence, the underlying request may be rejected.
      |No corruption of the ledger is to be expected.
      |This error is to be expected after a restart or failover of a mediator."""
  )
  @Resolution("Address the cause of the error. Let the submitter retry the command.")
  object InvalidMessage
      extends ErrorCode(
        "MEDIATOR_INVALID_MESSAGE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    override def logLevel: Level = Level.WARN

    final case class Reject(
        override val cause: String,
        _v0CodeP: v0.MediatorRejection.Code = v0.MediatorRejection.Code.Timeout,
    ) extends BaseCantonError.Impl(cause)
        with MediatorError {
      override def pretty: Pretty[Reject] = prettyOfClass(
        param("code", _.code.id.unquoted),
        param("cause", _.cause.unquoted),
      )
    }
  }

  /** Used as a fallback to represent mediator errors coming from a mediator running a higher version.
    * Only used for protocol versions from
    * [[com.digitalasset.canton.protocol.messages.Verdict.MediatorRejectV1.firstApplicableProtocolVersion]]
    * to [[com.digitalasset.canton.protocol.messages.Verdict.MediatorRejectV1.lastApplicableProtocolVersion]].
    *
    * @param id the id of the error code at the mediator. Only pass documented error code ids here, to avoid confusion.
    */
  final case class GenericError(
      override val cause: String,
      id: String,
      category: ErrorCategory,
      _v0CodeP: v0.MediatorRejection.Code = v0.MediatorRejection.Code.Timeout,
  ) extends BaseCantonError {
    override def code: ErrorCode = new ErrorCode(id, category) {}
  }

  @Explanation(
    """The mediator has received a malformed message. This may occur due to a bug at the sender of the message.
      |The message will be discarded. As a consequence, the underlying request may be rejected.
      |No corruption of the ledger is to be expected."""
  )
  @Resolution("Contact support.")
  object MalformedMessage extends AlarmErrorCode("MEDIATOR_RECEIVED_MALFORMED_MESSAGE") {

    final case class Reject(
        override val cause: String,
        _v0CodeP: v0.MediatorRejection.Code = v0.MediatorRejection.Code.Timeout,
    ) extends Alarm(cause)
        with MediatorError
        with BaseCantonError {
      override def pretty: Pretty[Reject] = prettyOfClass(
        param("code", _.code.id.unquoted),
        param("cause", _.cause.unquoted),
      )
    }
  }

  @Explanation(
    "Request processing failed due to a violation of internal invariants. It indicates a bug at the mediator."
  )
  @Resolution("Contact support.")
  object InternalError
      extends ErrorCode(
        "MEDIATOR_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    /** @param throwableO optional throwable that will not be serialized and is therefore not delivered to clients.
      */
    final case class Reject(
        override val cause: String,
        override val throwableO: Option[Throwable] = None,
    ) extends BaseCantonError.Impl(cause)
  }
}
