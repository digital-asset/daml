// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.daml.jwt.JwtVerifier
import com.daml.ledger.api.domain.JwksUrl

import scala.concurrent.Future

trait JwtVerifierLoader {
  def loadJwtVerifier(jwksUrl: JwksUrl, keyId: Option[String]): Future[JwtVerifier]
}
