// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava

import java.time.Clock
import java.util.UUID

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.auth.{
  AuthServiceStatic,
  Authorizer,
  Claim,
  ClaimActAsParty,
  ClaimAdmin,
  ClaimPublic,
  ClaimReadAsParty,
  Claims
}

package object grpc {

  private[grpc] val authorizer = new Authorizer(() => Clock.systemUTC().instant())

  private[grpc] val emptyToken = "empty"
  private[grpc] val publicToken = "public"
  private[grpc] val adminToken = "admin"

  private[grpc] val someParty = UUID.randomUUID.toString
  private[grpc] val somePartyReadToken = UUID.randomUUID.toString
  private[grpc] val somePartyReadWriteToken = UUID.randomUUID.toString

  private[grpc] val someOtherParty = UUID.randomUUID.toString
  private[grpc] val someOtherPartyReadToken = UUID.randomUUID.toString
  private[grpc] val someOtherPartyReadWriteToken = UUID.randomUUID.toString

  private[grpc] val mockedAuthService =
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
