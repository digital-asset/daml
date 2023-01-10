// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.security.KeyPair
import java.security.interfaces.{ECPrivateKey, ECPublicKey, RSAPrivateKey, RSAPublicKey}
import java.security.spec.ECGenParameterSpec

import com.auth0.jwt.algorithms.Algorithm
import org.scalactic.source
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.{\/, Show}
import scalaz.syntax.show._

class SignatureSpec extends AnyWordSpec with Matchers {

  import SignatureSpec._

  "Jwt" when {

    "using HMAC256 signatures" should {

      "work with a valid secret" in {
        val secret = "secret key"
        val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
        val jwtPayload = """{"dummy":"dummy"}"""
        val jwt = domain.DecodedJwt[String](jwtHeader, jwtPayload)

        val signedJwt = JwtSigner.HMAC256
          .sign(jwt, secret)
          .assertRight
        val verifier = HMAC256Verifier(secret).assertRight
        verifier
          .verify(signedJwt)
          .assertRight
      }

      "fail with an invalid secret" in {
        val secret = "secret key"
        val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
        val jwtPayload = """{"dummy":"dummy"}"""
        val jwt = domain.DecodedJwt[String](jwtHeader, jwtPayload)

        val success = {
          val signedJwt = JwtSigner.HMAC256
            .sign(jwt, secret)
            .assertRight
          val verifier = HMAC256Verifier("invalid " + secret).assertRight
          verifier
            .verify(signedJwt)
            .swap
            .leftMap(jwt => fail(s"JWT $jwt was unexpectedly verified"))
        }

        success.isRight shouldBe true
      }
    }

    "using RSA256 signatures" should {

      "work with a valid key" in {
        val kpg = java.security.KeyPairGenerator.getInstance("RSA")
        kpg.initialize(2048)
        val keyPair = kpg.generateKeyPair()
        val privateKey = keyPair.getPrivate.asInstanceOf[RSAPrivateKey]
        val publicKey = keyPair.getPublic.asInstanceOf[RSAPublicKey]

        val jwtHeader = """{"alg": "RS256", "typ": "JWT"}"""
        val jwtPayload = """{"dummy":"dummy"}"""
        val jwt = domain.DecodedJwt[String](jwtHeader, jwtPayload)

        val signedJwt = JwtSigner.RSA256
          .sign(jwt, privateKey)
          .assertRight
        val verifier = RSA256Verifier(publicKey).assertRight
        verifier
          .verify(signedJwt)
          .assertRight
      }

      "fail with an invalid key" in {
        val kpg = java.security.KeyPairGenerator.getInstance("RSA")
        kpg.initialize(2048)
        val keyPair1 = kpg.generateKeyPair()
        val privateKey = keyPair1.getPrivate.asInstanceOf[RSAPrivateKey]

        val keyPair2 = kpg.generateKeyPair()
        val publicKey = keyPair2.getPublic.asInstanceOf[RSAPublicKey]

        val jwtHeader = """{"alg": "RS256", "typ": "JWT"}"""
        val jwtPayload = """{"dummy":"dummy"}"""
        val jwt = domain.DecodedJwt[String](jwtHeader, jwtPayload)

        val signedJwt = JwtSigner.RSA256
          .sign(jwt, privateKey)
          .assertRight
        val verifier = RSA256Verifier(publicKey).assertRight
        verifier
          .verify(signedJwt)
          .swap
          .leftMap(jwt => fail(s"JWT $jwt was unexpectedly verified"))
      }
    }

    "using ECDA256 signatures" should {
      "work with a valid key" in {
        val kpg = java.security.KeyPairGenerator.getInstance("EC")
        val ecGenParameterSpec = new ECGenParameterSpec("secp256r1")
        kpg.initialize(ecGenParameterSpec)
        val keyPair: KeyPair = kpg.generateKeyPair()

        val privateKey = keyPair.getPrivate.asInstanceOf[ECPrivateKey]
        val publicKey = keyPair.getPublic.asInstanceOf[ECPublicKey]

        val jwtHeader = """{"alg": "ES256", "typ": "JWT"}"""
        val jwtPayload = """{"dummy":"dummy"}"""
        val jwt = domain.DecodedJwt[String](jwtHeader, jwtPayload)

        val signedJwt = JwtSigner.ECDSA
          .sign(jwt, privateKey, Algorithm.ECDSA256(null, _))
          .assertRight
        val verifier = ECDSAVerifier(Algorithm.ECDSA256(publicKey, null)).assertRight
        verifier
          .verify(signedJwt)
          .assertRight
      }
      "fail with a invalid key" in {
        val kpg = java.security.KeyPairGenerator.getInstance("EC")
        val ecGenParameterSpec = new ECGenParameterSpec("secp256r1")
        kpg.initialize(ecGenParameterSpec)
        val keyPair1: KeyPair = kpg.generateKeyPair()

        val privateKey1 = keyPair1.getPrivate.asInstanceOf[ECPrivateKey]

        val keyPair2: KeyPair = kpg.generateKeyPair()
        val publicKey2 = keyPair2.getPublic.asInstanceOf[ECPublicKey]

        val jwtHeader = """{"alg": "ES256", "typ": "JWT"}"""
        val jwtPayload = """{"dummy":"dummy"}"""
        val jwt = domain.DecodedJwt[String](jwtHeader, jwtPayload)
        val success = {
          val signedJwt = JwtSigner.ECDSA
            .sign(jwt, privateKey1, Algorithm.ECDSA256(null, _))
            .assertRight
          val verifier = ECDSAVerifier(Algorithm.ECDSA256(publicKey2, null)).assertRight
          verifier
            .verify(signedJwt)
            .swap
            .leftMap(jwt => fail(s"JWT $jwt was unexpectedly verified"))
        }

        success.isRight shouldBe true
      }
    }
    "using ECDSA512 signatures" should {
      "work with a valid key" in {
        val kpg = java.security.KeyPairGenerator.getInstance("EC")
        val ecGenParameterSpec = new ECGenParameterSpec("secp521r1")
        kpg.initialize(ecGenParameterSpec)
        val keyPair: KeyPair = kpg.generateKeyPair()

        val privateKey = keyPair.getPrivate.asInstanceOf[ECPrivateKey]
        val publicKey = keyPair.getPublic.asInstanceOf[ECPublicKey]

        val jwtHeader = """{"alg": "ES512", "typ": "JWT"}"""
        val jwtPayload = """{"dummy":"dummy"}"""
        val jwt = domain.DecodedJwt[String](jwtHeader, jwtPayload)
        val signedJwt = JwtSigner.ECDSA
          .sign(jwt, privateKey, Algorithm.ECDSA512(null, _))
          .assertRight

        val verifier = ECDSAVerifier(Algorithm.ECDSA512(publicKey, null)).assertRight
        verifier
          .verify(signedJwt)
          .assertRight
      }
      "fail with a invalid key" in {
        val kpg = java.security.KeyPairGenerator.getInstance("EC")
        val ecGenParameterSpec = new ECGenParameterSpec("secp521r1")
        kpg.initialize(ecGenParameterSpec)
        val keyPair1: KeyPair = kpg.generateKeyPair()

        val privateKey1 = keyPair1.getPrivate.asInstanceOf[ECPrivateKey]

        val keyPair2: KeyPair = kpg.generateKeyPair()
        val publicKey2 = keyPair2.getPublic.asInstanceOf[ECPublicKey]

        val jwtHeader = """{"alg": "ES512", "typ": "JWT"}"""
        val jwtPayload = """{"dummy":"dummy"}"""
        val jwt = domain.DecodedJwt[String](jwtHeader, jwtPayload)
        val success = {
          val signedJwt = JwtSigner.ECDSA
            .sign(jwt, privateKey1, Algorithm.ECDSA512(null, _))
            .assertRight
          val verifier = ECDSAVerifier(Algorithm.ECDSA512(publicKey2, null)).assertRight
          verifier
            .verify(signedJwt)
            .swap
            .leftMap(jwt => fail(s"JWT $jwt was unexpectedly verified"))
        }

        success.isRight shouldBe true
      }
    }

  }
}

object SignatureSpec {

  private implicit final class AssertRight[E, A](private val ea: E \/ A) extends AnyVal {
    def assertRight(implicit E: Show[E], pos: source.Position) =
      ea.valueOr(e => org.scalatest.Assertions.fail(e.shows))
  }

}
