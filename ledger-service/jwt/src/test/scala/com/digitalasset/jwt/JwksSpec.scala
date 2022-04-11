// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authenticity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import com.daml.testing.SimpleHttpServer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.\/
import scalaz.syntax.show._

import java.security.KeyPairGenerator
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}

class JwksSpec extends AnyFlatSpec with Matchers {

  val securityAsset: SecurityTest =
    SecurityTest(property = Authenticity, asset = "JWKS-configured Resource")

  def attack(threat: String): Attack = Attack(
    actor = s"JWKS-configured Resource User",
    threat = threat,
    mitigation = s"Refuse to verify authenticity of the token",
  )

  private def generateToken(
      keyId: String,
      privateKey: RSAPrivateKey,
      secondsTillExpiration: Long = 10L,
  ): JwtSigner.Error \/ Jwt = {
    val expiresIn = System.currentTimeMillis() / 1000L + secondsTillExpiration
    val jwtPayload = s"""{"test": "JwksSpec", "exp": $expiresIn}"""
    val jwtHeader = s"""{"alg": "RS256", "typ": "JWT", "kid": "$keyId"}"""
    JwtSigner.RSA256.sign(DecodedJwt(jwtHeader, jwtPayload), privateKey)
  }

  it should "successfully verify against provided correct key by JWKS server" taggedAs securityAsset
    .setHappyCase(
      "Successfully verify against provided correct key by JWKS server"
    ) in new JwksSpec.Scope {
    val token = generateToken("test-key-1", privateKey1)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result = verifier.verify(token)

    assert(
      result.isRight,
      s"The correctly signed token should successfully verify, but the result was ${result.leftMap(e => e.shows)}",
    )
  }

  it should "raise an error by verifying a token with an unknown key id" taggedAs securityAsset
    .setAttack(attack(threat = "Exploit an unknown key-id")) in new JwksSpec.Scope {
    val token = generateToken("test-key-unknown", privateKey1)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result = verifier.verify(token)

    assert(result.isLeft, s"The token with an unknown key ID should not successfully verify")
  }

  it should "raise an error by verifying a token with wrong public key" taggedAs securityAsset
    .setAttack(
      attack(threat = "Exploit a known key-id, but not the one used for the token encryption")
    ) in new JwksSpec.Scope {
    val token = generateToken("test-key-1", privateKey2)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result = verifier.verify(token)

    assert(
      result.isLeft,
      s"The token with a mismatching public key should not successfully verify",
    )
  }

  it should "invalidate cached key-id if verification failed" in new JwksSpec.Scope {
    val token = generateToken("test-key-1", privateKey1)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result = verifier.verify(token)

    assert(
      result.isRight,
      s"The correctly signed token should successfully verify, but the result was ${result.leftMap(e => e.shows)}",
    )

    server.stop(3)
    //now `test-key-1` is cached

    val updatedJwks = KeyUtils.generateJwks(
      Map(
        "test-key-2" -> publicKey2
      )
    )

    val updatedServer = SimpleHttpServer.start(updatedJwks, Some(server.getAddress.getPort))

    val expiredToken = generateToken("test-key-1", privateKey1, secondsTillExpiration = -100)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result2 = verifier.verify(expiredToken)

    assert(
      result2.isLeft,
      s"The expired but signed token should not successfully verify",
    )
    //now `test-key-1` must not be cached, but it's not part of JWKS, so it should be invalidated

    val token2 = generateToken("test-key-1", privateKey1, secondsTillExpiration = 100)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result3 = verifier.verify(token2)

    assert(
      result3.isLeft,
      s"The token with an unknown key ID (which was removed from Key Set) should not successfully verify",
    )
    updatedServer.stop(3)
  }
}

object JwksSpec {
  trait Scope {
    // Generate some RSA key pairs
    private val keySize = 2048
    private val kpg = KeyPairGenerator.getInstance("RSA")
    kpg.initialize(keySize)

    private val keyPair1 = kpg.generateKeyPair()
    private val publicKey1 = keyPair1.getPublic.asInstanceOf[RSAPublicKey]
    val privateKey1 = keyPair1.getPrivate.asInstanceOf[RSAPrivateKey]

    private val keyPair2 = kpg.generateKeyPair()
    val publicKey2 = keyPair2.getPublic.asInstanceOf[RSAPublicKey]
    val privateKey2 = keyPair2.getPrivate.asInstanceOf[RSAPrivateKey]

    // Start a JWKS server and create a verifier using the JWKS server
    private val jwks = KeyUtils.generateJwks(
      Map(
        "test-key-1" -> publicKey1,
        "test-key-2" -> publicKey2,
      )
    )

    val server = SimpleHttpServer.start(jwks)
    private val url = SimpleHttpServer.responseUrl(server)

    val verifier = JwksVerifier(url)
  }
}
