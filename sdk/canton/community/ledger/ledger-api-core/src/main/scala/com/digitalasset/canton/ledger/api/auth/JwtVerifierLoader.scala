// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.jwt.JwtVerifier
import com.digitalasset.canton.ledger.api.JwksUrl

import scala.concurrent.Future

trait JwtVerifierLoader {
  def loadJwtVerifier(jwksUrl: JwksUrl, keyId: Option[String]): Future[JwtVerifier]
}
