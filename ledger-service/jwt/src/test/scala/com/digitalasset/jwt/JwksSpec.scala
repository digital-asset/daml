// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.daml.jwt.domain.DecodedJwt
import com.daml.testing.SimpleHttpServer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.syntax.show._

import java.security.KeyPairGenerator
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}

class JwksSpec extends AnyFlatSpec with Matchers {

  private def generateToken(keyId: String, privateKey: RSAPrivateKey) = {
    val jwtPayload = s"""{"test": "JwksSpec"}"""
    val jwtHeader = s"""{"alg": "RS256", "typ": "JWT", "kid": "$keyId"}"""
    JwtSigner.RSA256.sign(DecodedJwt(jwtHeader, jwtPayload), privateKey)
  }

  it should "correctly verify JWT tokens using a JWKS server" in {
    // Generate some RSA key pairs
    val keySize = 2048
    val kpg = KeyPairGenerator.getInstance("RSA")
    kpg.initialize(keySize)

    val keyPair1 = kpg.generateKeyPair()
    val publicKey1 = keyPair1.getPublic.asInstanceOf[RSAPublicKey]
    val privateKey1 = keyPair1.getPrivate.asInstanceOf[RSAPrivateKey]

    val keyPair2 = kpg.generateKeyPair()
    val publicKey2 = keyPair2.getPublic.asInstanceOf[RSAPublicKey]
    val privateKey2 = keyPair2.getPrivate.asInstanceOf[RSAPrivateKey]

    // Start a JWKS server and create a verifier using the JWKS server
    val jwks = KeyUtils.generateJwks(
      Map(
        "test-key-1" -> publicKey1,
        "test-key-2" -> publicKey2,
      )
    )

    val server = SimpleHttpServer.start(jwks)
    val url = SimpleHttpServer.responseUrl(server)

    val verifier = JwksVerifier(url)

    // Test 1: Success
    val token1 = generateToken("test-key-1", privateKey1)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result1 = verifier.verify(token1)

    assert(
      result1.isRight,
      s"The correctly signed token should successfully verify, but the result was ${result1
        .leftMap(e => e.shows)}",
    )

    // Test 2: Failure - unknown key ID
    val token2 = generateToken("test-key-unknown", privateKey1)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result2 = verifier.verify(token2)

    assert(
      result2.isLeft,
      s"The token with an unknown key ID should not successfully verify",
    )

    // Test 3: Failure - wrong public key
    val token3 = generateToken("test-key-1", privateKey2)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result3 = verifier.verify(token3)

    assert(
      result3.isLeft,
      s"The token with a mismatching public key should not successfully verify",
    )

    SimpleHttpServer.stop(server)

    ()
  }
}
