// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.jwt.{HMAC256Verifier, JwtVerifier}
import com.daml.ledger.api.auth.JwtVerifierLoader
import com.daml.ledger.api.domain.JwksUrl
import com.daml.platform.sandbox.TestJwtVerifierLoader._

import scala.concurrent.Future

class TestJwtVerifierLoader() extends JwtVerifierLoader {
  private def verifier(secret: String): JwtVerifier =
    HMAC256Verifier(secret).getOrElse(sys.error("Failed to create HMAC256 verifier"))

  private val defaultJwtVerifier = verifier(secret1)

  private val verifierMap = Map(
    jwksUrl1 -> Future.successful(verifier(secret1)),
    jwksUrl2 -> Future.successful(verifier(secret2)),
    jwksUrl3 -> Future.failed(new VerifierException),
  )

  override def loadJwtVerifier(
      jwksUrl: JwksUrl,
      keyId: Option[String],
  ): Future[JwtVerifier] = verifierMap.getOrElse(jwksUrl, Future.successful(defaultJwtVerifier))
}

object TestJwtVerifierLoader {
  val jwksUrl1: JwksUrl = JwksUrl.assertFromString("http://daml.com/jwks1.json")
  val jwksUrl2: JwksUrl = JwksUrl.assertFromString("http://daml.com/jwks2.json")
  val jwksUrl3: JwksUrl = JwksUrl.assertFromString("http://daml.com/jwks3.json")

  val secret1: String = "secret1"
  val secret2: String = "secret2"

  class VerifierException extends Exception("Unable to find verifier")
}
