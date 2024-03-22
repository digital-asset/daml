// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authenticity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.\/
import scalaz.syntax.show._

import java.security.KeyPairGenerator
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}
import com.daml.http.test.SimpleHttpServer

class JwksSpec extends AnyFlatSpec with Matchers {

  val securityAsset: SecurityTest =
    SecurityTest(property = Authenticity, asset = "JWKS-configured Resource")

  def attack(threat: String): Attack = Attack(
    actor = s"JWKS-configured Resource User",
    threat = threat,
    mitigation = s"Refuse to verify authenticity of the token",
  )

  private def generateToken(keyId: String, privateKey: RSAPrivateKey): JwtSigner.Error \/ Jwt = {
    val jwtPayload = s"""{"test": "JwksSpec"}"""
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
    .setAttack(attack(threat = "Present an unknown key-id")) in new JwksSpec.Scope {
    val token = generateToken("test-key-unknown", privateKey1)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result = verifier.verify(token)

    assert(result.isLeft, s"The token with an unknown key ID should not successfully verify")
  }

  it should "raise an error by verifying a token with wrong public key" taggedAs securityAsset
    .setAttack(
      attack(threat = "Present a known key-id, but not the one used for the token encryption")
    ) in new JwksSpec.Scope {
    val token = generateToken("test-key-1", privateKey2)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result = verifier.verify(token)

    assert(
      result.isLeft,
      s"The token with a mismatching public key should not successfully verify",
    )
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
    private val publicKey2 = keyPair2.getPublic.asInstanceOf[RSAPublicKey]
    val privateKey2 = keyPair2.getPrivate.asInstanceOf[RSAPrivateKey]

    // Start a JWKS server and create a verifier using the JWKS server
    private val jwks = KeyUtils.generateJwks(
      Map(
        "test-key-1" -> publicKey1,
        "test-key-2" -> publicKey2,
      )
    )

    private val server = SimpleHttpServer.start(jwks)
    private val url = SimpleHttpServer.responseUrl(server)

    val verifier = JwksVerifier(url)
  }
}
