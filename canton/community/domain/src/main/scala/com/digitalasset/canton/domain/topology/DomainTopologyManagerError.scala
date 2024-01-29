// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.ProtocolVersion

sealed trait DomainTopologyManagerError extends CantonError with Product with Serializable
object DomainTopologyManagerError extends TopologyManagerError.DomainErrorGroup() {

  final case class TopologyManagerParentError(parent: TopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext
  ) extends DomainTopologyManagerError
      with ParentCantonError[TopologyManagerError]

  @Explanation(
    """This error indicates an external issue with the member addition hook."""
  )
  @Resolution("""Consult the error details.""")
  object FailedToAddMember
      extends ErrorCode(
        id = "FAILED_TO_ADD_MEMBER",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Failure(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "The add member hook failed"
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a domain topology manager attempts to activate a
      participant without having all necessary data, such as keys or domain trust certificates."""
  )
  @Resolution("""Register the necessary keys or trust certificates and try again.""")
  object ParticipantNotInitialized
      extends ErrorCode(
        id = "PARTICIPANT_NOT_INITIALIZED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(participantId: ParticipantId, currentKeys: KeyCollection)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The participant can not be enabled without registering the necessary keys first"
        )
        with DomainTopologyManagerError
    final case class Reject(participantId: ParticipantId, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(reason)
        with DomainTopologyManagerError
  }

  object InvalidOrFaultyOnboardingRequest
      extends AlarmErrorCode(id = "MALICOUS_OR_FAULTY_ONBOARDING_REQUEST") {

    final case class Failure(participantId: ParticipantId, reason: String)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(
          cause =
            "The participant submitted invalid or insufficient topology transactions during onboarding"
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction attempts to add keys for alien domain manager or sequencer entities to this domain topology manager."""
  )
  @Resolution(
    """Use a participant topology manager if you want to manage foreign domain keys"""
  )
  object AlienDomainEntities
      extends ErrorCode(
        id = "ALIEN_DOMAIN_ENTITIES",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(alienUid: UniqueIdentifier)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "Keys of alien domain entities can not be managed through a domain topology manager"
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction has a protocol version different than the one spoken on the domain."""
  )
  @Resolution(
    """Recreate the transaction with a correct protocol version."""
  )
  object WrongProtocolVersion
      extends ErrorCode(
        id = "WRONG_PROTOCOL_VERSION",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(
        domainProtocolVersion: ProtocolVersion,
        transactionProtocolVersion: ProtocolVersion,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            "Mismatch between protocol version of the transaction and the one spoken on the domain."
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction restricted to a domain should be added to another domain."""
  )
  @Resolution(
    """Recreate the content of the transaction with a correct domain identifier."""
  )
  object WrongDomain
      extends ErrorCode(id = "WRONG_DOMAIN", ErrorCategory.InvalidGivenCurrentSystemStateOther) {
    final case class Failure(wrongDomain: DomainId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Domain restricted transaction can not be added to different domain"
        )
        with DomainTopologyManagerError
  }

}
