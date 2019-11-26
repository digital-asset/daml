// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava

import java.time.Clock

import com.digitalasset.ledger.api.auth.{AuthServiceStatic, Authorizer, Claim, ClaimPublic, Claims}

package object grpc {

  private[grpc] val authorizer = new Authorizer(() => Clock.systemUTC().instant())

  private[grpc] val emptyToken = "empty"
  private[grpc] val publicToken = "public"

  private[grpc] val mockedAuthService =
    AuthServiceStatic {
      case `emptyToken` => Claims(Nil)
      case `publicToken` => Claims(Seq[Claim](ClaimPublic))
    }

}
