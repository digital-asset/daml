// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}

import org.scalatest.{Matchers, WordSpec}
import scalaz.syntax.show._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SignatureSpec extends WordSpec with Matchers {

  "Jwt" when {

    "using HMAC256 signatures" should {

      "work with a valid secret" in {
        val secret = "secret key"
        val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
        val jwtPayload = """{"dummy":"dummy"}"""
        val jwt = domain.DecodedJwt[String](jwtHeader, jwtPayload)

        val success = for {
          signedJwt <- JwtSigner.HMAC256
            .sign(jwt, secret)
            .leftMap(e => fail(e.shows))
          verifier <- HMAC256Verifier(secret)
            .leftMap(e => fail(e.shows))
          verifiedJwt <- verifier
            .verify(signedJwt)
            .leftMap(e => fail(e.shows))
        } yield verifiedJwt

        success.isRight shouldBe true
      }

      "fail with an invalid secret" in {
        val secret = "secret key"
        val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
        val jwtPayload = """{"dummy":"dummy"}"""
        val jwt = domain.DecodedJwt[String](jwtHeader, jwtPayload)

        val success = for {
          signedJwt <- JwtSigner.HMAC256
            .sign(jwt, secret)
            .leftMap(e => fail(e.shows))
          verifier <- HMAC256Verifier("invalid " + secret)
            .leftMap(e => fail(e.shows))
          error <- verifier
            .verify(signedJwt)
            .swap
            .leftMap(jwt => fail(s"JWT $jwt was unexpectedly verified"))
        } yield error

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

        val success = for {
          signedJwt <- JwtSigner.RSA256
            .sign(jwt, privateKey)
            .leftMap(e => fail(e.shows))
          verifier <- RSA256Verifier(publicKey)
            .leftMap(e => fail(e.shows))
          verifiedJwt <- verifier
            .verify(signedJwt)
            .leftMap(e => fail(e.shows))
        } yield verifiedJwt

        success.isRight shouldBe true
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

        val success = for {
          signedJwt <- JwtSigner.RSA256
            .sign(jwt, privateKey)
            .leftMap(e => fail(e.shows))
          verifier <- RSA256Verifier(publicKey)
            .leftMap(e => fail(e.shows))
          error <- verifier
            .verify(signedJwt)
            .swap
            .leftMap(jwt => fail(s"JWT $jwt was unexpectedly verified"))
        } yield error

        success.isRight shouldBe true
      }
    }
  }
}
