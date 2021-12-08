// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger

import com.daml.error.ErrorCodesVersionSwitcher

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
  ClaimSet,
}

package object rxjava {

  private[rxjava] def untestedEndpoint: Nothing =
    throw new UnsupportedOperationException("Untested endpoint, implement if needed")

  private[rxjava] val authorizer =
    Authorizer(
      () => Clock.systemUTC().instant(),
      "testLedgerId",
      "testParticipantId",
      new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes = true),
    )

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
      case `emptyToken` => ClaimSet.Unauthenticated
      case `publicToken` => ClaimSet.Claims.Empty.copy(claims = Seq[Claim](ClaimPublic))
      case `adminToken` => ClaimSet.Claims.Empty.copy(claims = Seq[Claim](ClaimAdmin))
      case `somePartyReadToken` =>
        ClaimSet.Claims.Empty.copy(
          claims = Seq[Claim](ClaimPublic, ClaimReadAsParty(Ref.Party.assertFromString(someParty)))
        )
      case `somePartyReadWriteToken` =>
        ClaimSet.Claims.Empty.copy(
          claims = Seq[Claim](ClaimPublic, ClaimActAsParty(Ref.Party.assertFromString(someParty)))
        )
      case `someOtherPartyReadToken` =>
        ClaimSet.Claims.Empty.copy(
          claims =
            Seq[Claim](ClaimPublic, ClaimReadAsParty(Ref.Party.assertFromString(someOtherParty)))
        )
      case `someOtherPartyReadWriteToken` =>
        ClaimSet.Claims.Empty.copy(
          claims =
            Seq[Claim](ClaimPublic, ClaimActAsParty(Ref.Party.assertFromString(someOtherParty)))
        )
    }

}
