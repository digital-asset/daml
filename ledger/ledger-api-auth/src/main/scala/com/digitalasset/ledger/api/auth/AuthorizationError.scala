// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.daml.ledger.api.domain.IdentityProviderId

import java.time.Instant

sealed abstract class AuthorizationError {
  def reason: String
}

object AuthorizationError {

  final case class Expired(authorizedUntil: Instant, currentTime: Instant)
      extends AuthorizationError {
    override val reason =
      s"Claims were valid until $authorizedUntil, current time is $currentTime"
  }

  case object ExpiredOnStream extends AuthorizationError {
    override val reason = "Claims have expired after the result stream has started"
  }

  final case class InvalidLedger(authorized: String, actual: String) extends AuthorizationError {
    override val reason =
      s"Claims are only valid for ledgerId '$authorized', actual ledgerId is '$actual'"
  }

  final case class InvalidParticipant(authorized: String, actual: String)
      extends AuthorizationError {
    override val reason =
      s"Claims are only valid for participantId '$authorized', actual participantId is '$actual'"
  }

  final case class InvalidApplication(authorized: String, actual: String)
      extends AuthorizationError {
    override val reason =
      s"Claims are only valid for applicationId '$authorized', actual applicationId is '$actual'"
  }

  case object MissingPublicClaim extends AuthorizationError {
    override val reason = "Claims do not authorize the use of public services"
  }

  case object MissingAdminClaim extends AuthorizationError {
    override val reason = "Claims do not authorize the use of administrative services."
  }

  final case class MissingReadClaim(party: String) extends AuthorizationError {
    override val reason = s"Claims do not authorize to read data for party '$party'"
  }

  final case class MissingActClaim(party: String) extends AuthorizationError {
    override val reason = s"Claims do not authorize to act as party '$party'"
  }

  final case class InvalidIdentityProviderIdArgument(error: String) extends AuthorizationError {
    override val reason =
      s"Invalid identityProviderId argument: '$error'."
  }

  final case class InvalidIdentityProviderId(identityProviderId: IdentityProviderId.Id)
      extends AuthorizationError {
    override val reason =
      s"Claims are only valid for identityProviderId '${identityProviderId.value}'."
  }
}
