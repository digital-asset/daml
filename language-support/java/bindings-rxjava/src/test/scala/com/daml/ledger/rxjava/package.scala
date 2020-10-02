// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger

import java.time.Clock
import java.util.UUID

import com.daml.lf.data.Ref
import com.daml.ledger.api.auth.{
  AuthServiceStatic,
  Authorizer,
  Claim,
  ClaimActAsParty,
  ClaimAdmin,
  ClaimPublic,
  ClaimReadAsParty,
  Claims
}

package object rxjava {

  private[rxjava] def untestedEndpoint: Nothing =
    throw new UnsupportedOperationException("Untested endpoint, implement if needed")

  private[rxjava] val authorizer =
    new Authorizer(() => Clock.systemUTC().instant(), "testLedgerId", "testParticipantId")

  private[rxjava] val emptyToken = "empty"
  private[rxjava] val publicToken = "public"
  private[rxjava] val adminToken = "admin"

  private[rxjava] val someParty = UUID.randomUUID.toString
  private[rxjava] val somePartyReadToken = UUID.randomUUID.toString
  private[rxjava] val somePartyReadWriteToken = UUID.randomUUID.toString

  private[rxjava] val someOtherParty = UUID.randomUUID.toString
  private[rxjava] val someOtherPartyReadToken = UUID.randomUUID.toString
  private[rxjava] val someOtherPartyReadWriteToken = UUID.randomUUID.toString

  private[rxjava] val mockedAuthService =
    AuthServiceStatic {
      case `emptyToken` => Claims(Nil)
      case `publicToken` => Claims(Seq[Claim](ClaimPublic))
      case `adminToken` => Claims(Seq[Claim](ClaimAdmin))
      case `somePartyReadToken` =>
        Claims(Seq[Claim](ClaimPublic, ClaimReadAsParty(Ref.Party.assertFromString(someParty))))
      case `somePartyReadWriteToken` =>
        Claims(Seq[Claim](ClaimPublic, ClaimActAsParty(Ref.Party.assertFromString(someParty))))
      case `someOtherPartyReadToken` =>
        Claims(
          Seq[Claim](ClaimPublic, ClaimReadAsParty(Ref.Party.assertFromString(someOtherParty))))
      case `someOtherPartyReadWriteToken` =>
        Claims(Seq[Claim](ClaimPublic, ClaimActAsParty(Ref.Party.assertFromString(someOtherParty))))
    }

}
