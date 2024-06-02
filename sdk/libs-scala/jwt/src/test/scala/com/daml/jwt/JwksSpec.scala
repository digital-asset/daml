// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import java.security.{KeyPairGenerator, PrivateKey, PublicKey}
import java.security.interfaces.{ECPrivateKey, ECPublicKey, RSAPrivateKey, RSAPublicKey}
import java.security.spec.ECGenParameterSpec

import com.auth0.jwt.algorithms.Algorithm
import com.daml.http.test.SimpleHttpServer

trait JwksSpec extends AnyFlatSpec with Matchers { self: JwksSpecKeys =>

  val securityAsset: SecurityTest =
    SecurityTest(property = Authenticity, asset = "JWKS-configured Resource")

  def attack(threat: String): Attack = Attack(
    actor = s"JWKS-configured Resource User",
    threat = threat,
    mitigation = s"Refuse to verify authenticity of the token",
  )

  it should "successfully verify against provided correct key by JWKS server" taggedAs securityAsset
    .setHappyCase(
      "Successfully verify against provided correct key by JWKS server"
    ) in {
    val token = generateToken("test-key-1", privateKey1)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result = verifier.verify(token)

    assert(
      result.isRight,
      s"The correctly signed token should successfully verify, but the result was ${result.leftMap(e => e.shows)}",
    )
  }

  it should "raise an error by verifying a token with an unknown key id" taggedAs securityAsset
    .setAttack(attack(threat = "Present an unknown key-id")) in {
    val token = generateToken("test-key-unknown", privateKey1)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result = verifier.verify(token)

    assert(result.isLeft, s"The token with an unknown key ID should not successfully verify")
  }

  it should "raise an error by verifying a token with wrong public key" taggedAs securityAsset
    .setAttack(
      attack(threat = "Present a known key-id, but not the one used for the token encryption")
    ) in {
    val token = generateToken("test-key-1", privateKey2)
      .fold(e => fail("Failed to generate signed token: " + e.shows), x => x)
    val result = verifier.verify(token)

    assert(
      result.isLeft,
      s"The token with a mismatching public key should not successfully verify",
    )
  }
}

trait JwksSpecKeys {

  protected type PublicKeyType <: PublicKey
  protected type PrivateKeyType <: PrivateKey

  protected def kpg: KeyPairGenerator
  protected def jwks: String
  protected def generateToken(keyId: String, privateKey: PrivateKeyType): JwtSigner.Error \/ Jwt

  // Generate some RSA key pairs
  private val keyPair1 = kpg.generateKeyPair()
  protected val publicKey1: PublicKeyType = keyPair1.getPublic.asInstanceOf[PublicKeyType]
  val privateKey1: PrivateKeyType = keyPair1.getPrivate.asInstanceOf[PrivateKeyType]

  private val keyPair2 = kpg.generateKeyPair()
  protected val publicKey2: PublicKeyType = keyPair2.getPublic.asInstanceOf[PublicKeyType]
  val privateKey2: PrivateKeyType = keyPair2.getPrivate.asInstanceOf[PrivateKeyType]

  private val server = SimpleHttpServer.start(jwks)
  private val url = SimpleHttpServer.responseUrl(server)

  protected val verifier: JwksVerifier = JwksVerifier(url)
}

class JwksSpecRSA extends JwksSpec with JwksSpecKeys {

  private val keySize = 2048

  override type PublicKeyType = RSAPublicKey
  override type PrivateKeyType = RSAPrivateKey

  override def kpg: KeyPairGenerator = KeyPairGenerator.getInstance("RSA")
  kpg.initialize(keySize)

  override def jwks: String = KeyUtils.generateJwks(
    Map(
      "test-key-1" -> publicKey1,
      "test-key-2" -> publicKey2,
    )
  )

  override def generateToken(keyId: String, privateKey: PrivateKeyType): JwtSigner.Error \/ Jwt = {
    val jwtPayload = s"""{"test": "JwksSpec"}"""
    val jwtHeader = s"""{"alg": "RS256", "typ": "JWT", "kid": "$keyId"}"""
    JwtSigner.RSA256.sign(DecodedJwt(jwtHeader, jwtPayload), privateKey)
  }
}

class JwksSpecES256 extends JwksSpec with JwksSpecKeys {

  override type PublicKeyType = ECPublicKey
  override type PrivateKeyType = ECPrivateKey

  protected def kpg: KeyPairGenerator = {
    val gen = KeyPairGenerator.getInstance("EC")
    gen.initialize(new ECGenParameterSpec("secp256r1"))
    gen
  }

  protected def jwks: String = KeyUtils.generateECJwks(
    Map(
      "test-key-1" -> publicKey1,
      "test-key-2" -> publicKey2,
    )
  )

  protected def generateToken(keyId: String, privateKey: PrivateKeyType): JwtSigner.Error \/ Jwt = {
    val jwtPayload = s"""{"test": "JwksSpec"}"""
    val jwtHeader = s"""{"alg": "ES256", "typ": "JWT", "kid": "$keyId"}"""
    JwtSigner.ECDSA.sign(DecodedJwt(jwtHeader, jwtPayload), privateKey, Algorithm.ECDSA256(null, _))
  }
}

class JwksSpecES512 extends JwksSpec with JwksSpecKeys {

  override type PublicKeyType = ECPublicKey
  override type PrivateKeyType = ECPrivateKey

  protected def kpg: KeyPairGenerator = {
    val gen = KeyPairGenerator.getInstance("EC")
    gen.initialize(new ECGenParameterSpec("secp521r1"))
    gen
  }

  protected def jwks: String = KeyUtils.generateECJwks(
    Map(
      "test-key-1" -> publicKey1,
      "test-key-2" -> publicKey2,
    )
  )

  protected def generateToken(keyId: String, privateKey: PrivateKeyType): JwtSigner.Error \/ Jwt = {
    val jwtPayload = s"""{"test": "JwksSpec"}"""
    val jwtHeader = s"""{"alg": "ES512", "typ": "JWT", "kid": "$keyId"}"""
    JwtSigner.ECDSA.sign(DecodedJwt(jwtHeader, jwtPayload), privateKey, Algorithm.ECDSA512(null, _))
  }
}
