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
  ClaimPublic,
  ClaimReadAsParty,
  Claims
}

package object grpc {

  private[grpc] val authorizer = new Authorizer(() => Clock.systemUTC().instant())

  private[grpc] val emptyToken = "empty"
  private[grpc] val publicToken = "public"

  private[grpc] val someParty = UUID.randomUUID.toString
  private[grpc] val somePartyToken = UUID.randomUUID.toString

  private[grpc] val mockedAuthService =
    AuthServiceStatic {
      case `emptyToken` => Claims(Nil)
      case `publicToken` => Claims(Seq[Claim](ClaimPublic))
      case `somePartyToken` =>
        Claims(Seq[Claim](ClaimPublic, ClaimReadAsParty(Ref.Party.assertFromString(someParty))))
    }

}
