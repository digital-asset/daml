// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.{JwksUrl, JwtVerifier}

import scala.concurrent.Future

trait JwtVerifierLoader {
  def loadJwtVerifier(jwksUrl: JwksUrl, keyId: Option[String]): Future[JwtVerifier]
}
