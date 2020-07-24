// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant

sealed abstract class AuthorizationError {
  def reason: String
}

final case class AuthorizationErrorExpired(authorizedUntil: Instant, currentTime: Instant)
    extends AuthorizationError {
  override def reason =
    s"Expired. Claims were valid until $authorizedUntil, current time is $currentTime"
}

final case class AuthorizationErrorExpiredOnStream() extends AuthorizationError {
  override def reason = s"Claims have expired after the result stream has started"
}

final case class AuthorizationErrorInvalidLedger(authorized: String, actual: String)
    extends AuthorizationError {
  override def reason =
    s"Invalid ledger. Claims are only valid for ledgerId $authorized, actual ledgerId is $actual"
}

final case class AuthorizationErrorInvalidParticipant(authorized: String, actual: String)
    extends AuthorizationError {
  override def reason =
    s"Invalid participant. Claims are only valid for participantId $authorized, actual participantId is $actual"
}

final case class AuthorizationErrorInvalidApplication(authorized: String, actual: String)
    extends AuthorizationError {
  override def reason =
    s"Invalid application. Claims are only valid for applicationId $authorized, actual applicationId is $actual"
}

final case class AuthorizationErrorMissingPublicClaim() extends AuthorizationError {
  override def reason = s"Missing public claim. Claims do not authorize the use of public services"
}

final case class AuthorizationErrorMissingAdminClaim() extends AuthorizationError {
  override def reason =
    s"Missing admin claim. Claims do not authorize the use of administrative services"
}

final case class AuthorizationErrorMissingReadClaim(party: String) extends AuthorizationError {
  override def reason = s"Missing read claim. Claims do not authorize to read data for party $party"
}

final case class AuthorizationErrorMissingActClaim(party: String) extends AuthorizationError {
  override def reason = s"Missing act claim. Claims do not authorize to act as party $party"
}
