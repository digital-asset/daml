// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.security.interfaces.{ECPrivateKey, ECPublicKey, RSAPrivateKey, RSAPublicKey}
import java.security.spec.ECGenParameterSpec
import java.time.temporal.ChronoUnit
import java.util.Date

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.exceptions.{InvalidClaimException, TokenExpiredException}
import org.scalactic.source
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import scalaz.{\/, Show}
import scalaz.syntax.show._

class JwtTimestampLeewaySpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import JwtTimestampLeewaySpec._

  "Jwt" when {
    forAll(verifires) { (verifierType, algorithm, jwtVerifier) =>
      ("using " + verifierType + " verifier") should {

        "work with a token that has not expired" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withExpiresAt(fiveSecondsLaterFrom(now))
            .sign(algorithm)

          jwtVerifier(None).verifier
            .verify(token)
        }

        "work with an expired token when leeway overlaps verification time" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withExpiresAt(oneSecondEarlierFrom(now))
            .sign(algorithm)

          val leeway1 = Some(JwtTimestampLeeway(Some(5), None, None, None))
          jwtVerifier(leeway1).verifier
            .verify(token)
        }

        "work with an expired token when expiresAt overlaps verification time" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withExpiresAt(oneSecondEarlierFrom(now))
            .sign(algorithm)

          val expiresAt1 = Some(JwtTimestampLeeway(None, Some(5), None, None))
          jwtVerifier(expiresAt1).verifier
            .verify(token)
        }

        "fail with an expired token when leeway is off" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withExpiresAt(oneSecondEarlierFrom(now))
            .sign(algorithm)

          assertThrows[TokenExpiredException] {
            jwtVerifier(None).verifier
              .verify(token)
          }
        }

        "fail with an expired token when leeway does not overlap verification time" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withExpiresAt(fiveSecondsEarlierFrom(now))
            .sign(algorithm)

          val leeway1 = Some(JwtTimestampLeeway(Some(1), None, None, None))
          assertThrows[TokenExpiredException] {
            jwtVerifier(leeway1).verifier
              .verify(token)
          }
        }

        "work with an expired token when leeway does not overlap verification but expiresAt does" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withExpiresAt(fiveSecondsEarlierFrom(now))
            .sign(algorithm)

          val leeway = Some(JwtTimestampLeeway(Some(1), Some(10), None, None))
          jwtVerifier(leeway).verifier
            .verify(token)
        }

        "fail with an expired token when leeway overlaps verification time but expiresAt does not" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withExpiresAt(fiveSecondsEarlierFrom(now))
            .sign(algorithm)

          val leeway = Some(JwtTimestampLeeway(Some(10), Some(1), None, None))
          assertThrows[TokenExpiredException] {
            jwtVerifier(leeway).verifier
              .verify(token)
          }
        }

        "work with a token issued in a past date" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withIssuedAt(fiveSecondsEarlierFrom(now))
            .sign(algorithm)

          jwtVerifier(None).verifier
            .verify(token)
        }

        "work with a token issued in a future date when leeway overlaps verification time" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withIssuedAt(oneSecondLaterFrom(now))
            .sign(algorithm)

          val leeway1 = Some(JwtTimestampLeeway(Some(5), None, None, None))
          jwtVerifier(leeway1).verifier
            .verify(token)
        }

        "work with a token issued in a future date when issuedAt overlaps verification time" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withIssuedAt(oneSecondLaterFrom(now))
            .sign(algorithm)

          val issuedAt1 = Some(JwtTimestampLeeway(None, None, Some(5), None))
          jwtVerifier(issuedAt1).verifier
            .verify(token)
        }

        "fail with a token issued in a future date when leeway is off" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withIssuedAt(oneSecondLaterFrom(now))
            .sign(algorithm)

          assertThrows[InvalidClaimException] {
            jwtVerifier(None).verifier
              .verify(token)
          }
        }

        "fail with a token issued in a future date when leeway does not overlap verification time" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withIssuedAt(fiveSecondsLaterFrom(now))
            .sign(algorithm)

          val leeway1 = Some(JwtTimestampLeeway(Some(1), None, None, None))
          assertThrows[InvalidClaimException] {
            jwtVerifier(leeway1).verifier
              .verify(token)
          }
        }

        "work with a token issued in a future date when leeway does not overlap verification but issuedAt does" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withIssuedAt(fiveSecondsLaterFrom(now))
            .sign(algorithm)

          val leeway = Some(JwtTimestampLeeway(Some(1), None, Some(10), None))
          jwtVerifier(leeway).verifier
            .verify(token)
        }

        "fail with a token issued in a future date when leeway overlaps verification time but expiresAt does not" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withIssuedAt(fiveSecondsLaterFrom(now))
            .sign(algorithm)

          val leeway = Some(JwtTimestampLeeway(Some(10), None, Some(1), None))
          assertThrows[InvalidClaimException] {
            jwtVerifier(leeway).verifier
              .verify(token)
          }
        }

        "work with a token that can already be used" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withNotBefore(oneSecondEarlierFrom(now))
            .sign(algorithm)

          jwtVerifier(None).verifier
            .verify(token)
        }

        "work with a token usable in a future date when leeway overlaps verification time" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withNotBefore(oneSecondLaterFrom(now))
            .sign(algorithm)

          val leeway1 = Some(JwtTimestampLeeway(Some(5), None, None, None))
          jwtVerifier(leeway1).verifier
            .verify(token)
        }

        "work with a token usable in a future date when notBefore overlaps verification time" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withNotBefore(oneSecondLaterFrom(now))
            .sign(algorithm)

          val notBefore1 = Some(JwtTimestampLeeway(None, None, None, Some(5)))
          jwtVerifier(notBefore1).verifier
            .verify(token)
        }

        "fail with a token usable in a future date when leeway is off" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withNotBefore(fiveSecondsLaterFrom(now))
            .sign(algorithm)

          assertThrows[InvalidClaimException] {
            jwtVerifier(None).verifier
              .verify(token)
          }
        }

        "fail with a token usable in a future date when leeway does not overlap verification time" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withNotBefore(fiveSecondsLaterFrom(now))
            .sign(algorithm)

          val leeway1 = Some(JwtTimestampLeeway(Some(1), None, None, None))
          assertThrows[InvalidClaimException] {
            jwtVerifier(leeway1).verifier
              .verify(token)
          }
        }

        "work with a token usable in a future date when leeway does not overlap verification but notBefore does" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withNotBefore(fiveSecondsLaterFrom(now))
            .sign(algorithm)

          val leeway = Some(JwtTimestampLeeway(Some(1), None, None, Some(10)))
          jwtVerifier(leeway).verifier
            .verify(token)
        }

        "fail with a token usable in a future date when leeway overlaps verification time but notBefore does not" in {
          val now = new Date()
          val token: String = JWT
            .create()
            .withNotBefore(fiveSecondsLaterFrom(now))
            .sign(algorithm)

          val leeway = Some(JwtTimestampLeeway(Some(10), None, None, Some(1)))
          assertThrows[InvalidClaimException] {
            jwtVerifier(leeway).verifier
              .verify(token)
          }
        }
      }
    }
  }
}

object JwtTimestampLeewaySpec extends TableDrivenPropertyChecks {

  // HMAC
  val secret = "secret key"

  // RSA
  val kpgRSA = java.security.KeyPairGenerator.getInstance("RSA")
  kpgRSA.initialize(2048)
  val keyPairRSA = kpgRSA.generateKeyPair()
  val privateKeyRSA = keyPairRSA.getPrivate.asInstanceOf[RSAPrivateKey]
  val publicKeyRSA = keyPairRSA.getPublic.asInstanceOf[RSAPublicKey]

  // ECDSA
  // 256
  val kpgECDSA256 = java.security.KeyPairGenerator.getInstance("EC")
  val ecGenParameterSpec256 = new ECGenParameterSpec("secp256r1")
  kpgECDSA256.initialize(ecGenParameterSpec256)
  val keyPairECDSA256 = kpgECDSA256.generateKeyPair()
  val privateKeyECDSA256 = keyPairECDSA256.getPrivate.asInstanceOf[ECPrivateKey]
  val publicKeyECDSA256 = keyPairECDSA256.getPublic.asInstanceOf[ECPublicKey]
  // 512
  val kpg512 = java.security.KeyPairGenerator.getInstance("EC")
  val ecGenParameterSpec512 = new ECGenParameterSpec("secp521r1")
  kpg512.initialize(ecGenParameterSpec512)
  val keyPairECDSA512 = kpg512.generateKeyPair()
  val privateKeyECDSA512 = keyPairECDSA512.getPrivate.asInstanceOf[ECPrivateKey]
  val publicKeyECDSA512 = keyPairECDSA512.getPublic.asInstanceOf[ECPublicKey]

  val hmac256Verifier_ = HMAC256Verifier(secret, _: Option[JwtTimestampLeeway]).assertRight
  val rsa256Verifier_ = RSA256Verifier(publicKeyRSA, _: Option[JwtTimestampLeeway]).assertRight
  val ecdsa256Verifier_ =
    ECDSAVerifier(
      Algorithm.ECDSA256(publicKeyECDSA256, null),
      _: Option[JwtTimestampLeeway],
    ).assertRight
  val ecdsa512Verifier_ =
    ECDSAVerifier(
      Algorithm.ECDSA512(publicKeyECDSA512, null),
      _: Option[JwtTimestampLeeway],
    ).assertRight

  val verifires = Table(
    ("verifier name", "algorithm", "verifier"),
    ("HMAC 256", Algorithm.HMAC256(secret), hmac256Verifier_),
    ("RSA 256", Algorithm.RSA256(publicKeyRSA, privateKeyRSA), rsa256Verifier_),
    ("ECDSA 256", Algorithm.ECDSA256(publicKeyECDSA256, privateKeyECDSA256), ecdsa256Verifier_),
    ("ECDSA 512", Algorithm.ECDSA512(publicKeyECDSA512, privateKeyECDSA512), ecdsa512Verifier_),
  )

  def oneSecondEarlierFrom(date: Date): Date =
    Date.from(date.toInstant.minus(1, ChronoUnit.SECONDS))

  def oneSecondLaterFrom(date: Date): Date =
    Date.from(date.toInstant.plus(1, ChronoUnit.SECONDS))

  def fiveSecondsEarlierFrom(date: Date): Date =
    Date.from(date.toInstant.minus(5, ChronoUnit.SECONDS))

  def fiveSecondsLaterFrom(date: Date): Date =
    Date.from(date.toInstant.plus(5, ChronoUnit.SECONDS))

  private implicit final class AssertRight[E, A](private val ea: E \/ A) extends AnyVal {
    def assertRight(implicit E: Show[E], pos: source.Position) =
      ea.valueOr(e => org.scalatest.Assertions.fail(e.shows))
  }

}
