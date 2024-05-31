// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication

import cats.implicits.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.{Nonce, Signature}
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.{
  MemberAccessDisabled,
  MissingToken,
  NonMatchingDomainId,
}
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.sequencing.authentication.{AuthenticationToken, MemberAuthentication}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration as JDuration
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class MemberAuthenticationServiceTest extends AsyncWordSpec with BaseTest {

  import DefaultTestIdentities.*

  val p1 = participant1

  val clock: SimClock = new SimClock(loggerFactory = loggerFactory)

  val topology = TestingTopology().withSimpleParticipants(participant1).build()
  val syncCrypto = topology.forOwnerAndDomain(participant1, domainId)

  def service(
      participantIsActive: Boolean,
      useExponentialRandomTokenExpiration: Boolean = false,
      nonceDuration: JDuration = JDuration.ofMinutes(1),
      tokenDuration: JDuration = JDuration.ofHours(1),
      invalidateMemberCallback: Member => Unit = _ => (),
      store: MemberAuthenticationStore = new InMemoryMemberAuthenticationStore(),
  ): MemberAuthenticationService =
    new MemberAuthenticationService(
      domainId,
      syncCrypto,
      store,
      clock,
      nonceDuration,
      tokenDuration,
      useExponentialRandomTokenExpiration = useExponentialRandomTokenExpiration,
      memberT => invalidateMemberCallback(memberT.value),
      Future.unit,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    ) {
      override def isParticipantActive(participant: ParticipantId)(implicit
          traceContext: TraceContext
      ): Future[Boolean] =
        Future.successful(participantIsActive)
    }

  def getMemberAuthentication(member: Member) =
    MemberAuthentication(member).getOrElse(fail("unsupported"))

  "ParticipantAuthenticationService" should {

    def generateToken(sut: MemberAuthenticationService) =
      for {
        challenge <- sut.generateNonce(p1)
        (nonce, fingerprints) = challenge
        signature <- getMemberAuthentication(p1)
          .signDomainNonce(p1, nonce, domainId, fingerprints, syncCrypto.crypto)
          .failOnShutdown
        tokenAndExpiry <- sut.validateSignature(p1, signature, nonce)
      } yield tokenAndExpiry

    "generate nonce, verify signature, generate token, verify token, and verify expiry" in {
      val sut = service(participantIsActive = true)
      (for {
        tokenAndExpiry <- generateToken(sut)
        AuthenticationTokenWithExpiry(token, expiry) = tokenAndExpiry
        _ <- sut.validateToken(domainId, p1, token)
      } yield expiry).value.map {
        case Right(expiry) =>
          expiry should be(clock.now.plus(JDuration.ofHours(1)))
        case Left(error) => fail(s"Failed with error: $error")
      }
    }

    "generate nonce, verify signature, generate token, verify token, and verify exponential expiry" in {
      val sut = service(participantIsActive = true, useExponentialRandomTokenExpiration = true)
      (for {
        tokenAndExpiry <- generateToken(sut)
        AuthenticationTokenWithExpiry(token, expiry) = tokenAndExpiry
        _ <- sut.validateToken(domainId, p1, token)
      } yield expiry).value.map {
        case Right(expiry) =>
          expiry should be >= clock.now.plus(JDuration.ofMinutes(30))
          expiry should be <= clock.now.plus(JDuration.ofHours(1))
        case Left(error) => fail(s"Failed with error: $error")
      }
    }

    "use random expiry" in {
      val sut = service(participantIsActive = true, useExponentialRandomTokenExpiration = true)
      Seq
        .fill(10) {
          generateToken(sut).map(_.expiresAt)
        }
        .sequence
        .value
        .map {
          case Right(expireTimes) =>
            expireTimes.distinct.size should be > 1
          case Left(error) => fail(s"Failed with error: $error")
        }
    }

    "should fail every method if participant is not active" in {
      val sut = service(false)
      for {
        generateNonceError <- leftOrFail(sut.generateNonce(p1))("generating nonce")
        validateSignatureError <- leftOrFail(
          sut.validateSignature(p1, null, Nonce.generate(syncCrypto.pureCrypto))
        )(
          "validateSignature"
        )
        validateTokenError <- leftOrFail(sut.validateToken(domainId, p1, null))(
          "token validation should fail"
        )
      } yield {
        generateNonceError shouldBe MemberAccessDisabled(p1)
        validateSignatureError shouldBe MemberAccessDisabled(p1)
        validateTokenError shouldBe MissingToken(p1)
      }
    }

    "should check whether the intended domain is the one the participant is connecting to" in {
      val sut = service(false)
      val wrongDomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong::domain"))

      for {
        error <- leftOrFail(sut.validateToken(wrongDomainId, p1, null))("should fail domain check")
      } yield error shouldBe NonMatchingDomainId(p1, wrongDomainId)
    }

    "properly handle becoming a passive node" in {
      val sut = service(true, store = new PassiveSequencerMemberAuthenticationStore())

      for {
        generateNonceError <- leftOrFail(sut.generateNonce(p1))("generateNonce should fail")
        validateTokenError <-
          leftOrFail(
            sut.validateToken(
              domainId,
              p1,
              AuthenticationToken.generate(syncCrypto.crypto.pureCrypto),
            )
          )("validateToken should fail")

        validateSignatureError <- leftOrFail(
          sut.validateSignature(
            p1,
            Signature.noSignature,
            Nonce.generate(syncCrypto.crypto.pureCrypto),
          )
        )("validateSignature should fail")
      } yield {
        generateNonceError shouldBe MemberAuthentication.PassiveSequencer
        validateTokenError shouldBe MemberAuthentication.PassiveSequencer
        validateSignatureError shouldBe MemberAuthentication.PassiveSequencer
      }
    }
  }
}
