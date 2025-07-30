// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.digitalasset.base.error.{
  Alarm,
  AlarmErrorCode,
  ErrorCategory,
  ErrorCode,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.error.CantonErrorGroups.MediatorErrorGroup
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.ParticipantId
import org.slf4j.event.Level

sealed trait MediatorError extends Product with Serializable with PrettyPrinting {
  def isMalformed: Boolean
}

object MediatorError extends MediatorErrorGroup {

  @Explanation(
    """This rejection indicates that the transaction has been rejected by the mediator as it didn't receive enough confirmations within the confirmation response timeout.
      The field "unresponsiveParties" in the error info contains the comma-separated list of parties that failed to send a response within the confirmation response timeout. This field is only present since protocol version 6"""
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
    ) extends CantonBaseError.Impl(cause)
        with MediatorError {

      override def isMalformed: Boolean = false

      override protected def pretty: Pretty[Reject] = prettyOfClass(
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
      private val defaultCause: String =
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
        override val cause: String
    ) extends CantonBaseError.Impl(cause)
        with MediatorError {

      override def isMalformed: Boolean = false

      override protected def pretty: Pretty[Reject] = prettyOfClass(
        param("code", _.code.id.unquoted),
        param("cause", _.cause.unquoted),
      )
    }
  }

  @Explanation(
    """The mediator has received a malformed message. This may occur due to a bug at the sender of the message.
      |The message will be discarded. As a consequence, the underlying request may be rejected.
      |No corruption of the ledger is to be expected."""
  )
  @Resolution("Contact support.")
  object MalformedMessage extends AlarmErrorCode("MEDIATOR_RECEIVED_MALFORMED_MESSAGE") {

    final case class Reject(
        override val cause: String
    ) extends Alarm(cause)
        with MediatorError
        with CantonBaseError {

      override def isMalformed: Boolean = true

      override protected def pretty: Pretty[Reject] = prettyOfClass(
        param("code", _.code.id.unquoted),
        param("cause", _.cause.unquoted),
      )
    }
  }

  @Explanation(
    """A participant has equivocated by submitting contradictory verdicts (Reject and Approve, or vice versa)
      |for the same request and view. This is a serious security concern indicating either a malicious participant or a
      |critical bug in the participant's software. The second verdict is ignored.
      |No corruption of the ledger is to be expected, but this incident should be investigated."""
  )
  @Resolution("Investigate the participant for misbehavior. Contact support.")
  object ParticipantEquivocation extends AlarmErrorCode("MEDIATOR_PARTICIPANT_EQUIVOCATION") {

    final case class Detected(
        override val cause: String,
        participantId: ParticipantId,
    ) extends Alarm(cause)
        with PrettyPrinting
        with CantonBaseError {

      override protected def pretty: Pretty[Detected] = prettyOfClass(
        param("code", _.code.id.unquoted),
        param("cause", _.cause.unquoted),
        param("participantId", _.participantId),
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

    /** @param throwableO
      *   optional throwable that will not be serialized and is therefore not delivered to clients.
      */
    final case class Reject(
        override val cause: String,
        override val throwableO: Option[Throwable] = None,
    ) extends CantonBaseError.Impl(cause)
  }
}
