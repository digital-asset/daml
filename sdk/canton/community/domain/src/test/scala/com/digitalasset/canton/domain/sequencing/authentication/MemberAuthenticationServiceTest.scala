// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication

import cats.data.EitherT
import cats.implicits.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.Nonce
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.{
  AuthenticationError,
  MemberAccessDisabled,
  MissingToken,
  NonMatchingDomainId,
}
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration as JDuration
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class MemberAuthenticationServiceTest extends AsyncWordSpec with BaseTest {

  import DefaultTestIdentities.*

  private val p1 = participant1

  private val clock: SimClock = new SimClock(loggerFactory = loggerFactory)

  private val topology = TestingTopology().withSimpleParticipants(participant1).build()
  private val syncCrypto = topology.forOwnerAndDomain(participant1, domainId)

  private def service(
      participantIsActive: Boolean,
      useExponentialRandomTokenExpiration: Boolean = false,
      nonceDuration: JDuration = JDuration.ofMinutes(1),
      tokenDuration: JDuration = JDuration.ofHours(1),
      invalidateMemberCallback: Member => Unit = _ => (),
      store: MemberAuthenticationStore = new MemberAuthenticationStore(),
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

  private def getMemberAuthentication(member: Member): MemberAuthentication =
    MemberAuthentication(member).getOrElse(fail("unsupported"))

  "MemberAuthenticationService" should {

    def generateToken(sut: MemberAuthenticationService) =
      for {
        challenge <- sut.generateNonce(p1)
        (nonce, fingerprints) = challenge
        signature <- getMemberAuthentication(p1)
          .signDomainNonce(p1, nonce, domainId, fingerprints, syncCrypto.crypto)
          .failOnShutdown
        tokenAndExpiry <- sut.validateSignature(p1, signature, nonce)
      } yield tokenAndExpiry

    def fetchTokens(
        store: MemberAuthenticationStore,
        members: Seq[Member],
    ): Map[Member, Seq[StoredAuthenticationToken]] =
      members.flatMap(store.fetchTokens).groupBy(_.member)

    "generate nonce, verify signature, generate token, verify token, and verify expiry" in {
      val sut = service(participantIsActive = true)
      for {
        tokenAndExpiry <- generateToken(sut)
        AuthenticationTokenWithExpiry(token, expiry) = tokenAndExpiry
        _ <- EitherT.fromEither[Future](sut.validateToken(domainId, p1, token))
      } yield {
        expiry should be(clock.now.plus(JDuration.ofHours(1)))
      }
    }

    "generate nonce, verify signature, generate token, verify token, and verify exponential expiry" in {
      val sut = service(participantIsActive = true, useExponentialRandomTokenExpiration = true)
      for {
        tokenAndExpiry <- generateToken(sut)
        AuthenticationTokenWithExpiry(token, expiry) = tokenAndExpiry
        _ <- EitherT.fromEither[Future](sut.validateToken(domainId, p1, token))
      } yield {
        expiry should be >= clock.now.plus(JDuration.ofMinutes(30))
        expiry should be <= clock.now.plus(JDuration.ofHours(1))
      }
    }

    "use random expiry" in {
      val sut = service(participantIsActive = true, useExponentialRandomTokenExpiration = true)
      for {
        expireTimes <- Seq.fill(10)(generateToken(sut).map(_.expiresAt)).sequence
      } yield {
        expireTimes.distinct.size should be > 1
      }
    }

    "fail every method if participant is not active" in {
      val sut = service(participantIsActive = false)
      for {
        generateNonceError <- leftOrFail(sut.generateNonce(p1))("generating nonce")
        validateSignatureError <- leftOrFail(
          sut.validateSignature(p1, null, Nonce.generate(syncCrypto.pureCrypto))
        )(
          "validateSignature"
        )
        validateTokenError = leftOrFail(sut.validateToken(domainId, p1, null))(
          "token validation should fail"
        )
      } yield {
        generateNonceError shouldBe MemberAccessDisabled(p1)
        validateSignatureError shouldBe MemberAccessDisabled(p1)
        validateTokenError shouldBe MissingToken(p1)
      }
    }

    "check whether the intended domain is the one the participant is connecting to" in {
      val sut = service(participantIsActive = false)
      val wrongDomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong::domain"))

      val error = leftOrFail(sut.validateToken(wrongDomainId, p1, null))("should fail domain check")
      error shouldBe NonMatchingDomainId(p1, wrongDomainId)
    }

    "invalidate all tokens from a member when logging out" in {
      val store = new MemberAuthenticationStore()
      val sut = service(participantIsActive = true, store = store)

      for {
        tokenAndExpiry <- generateToken(sut)
        AuthenticationTokenWithExpiry(token, _expiry) = tokenAndExpiry
        _ <- EitherT.fromEither[Future](sut.validateToken(domainId, p1, token))
        // Generate a second token for p1
        _ <- generateToken(sut)

        tokensBefore = fetchTokens(store, Seq(p1))

        // Use the first token to invalidate them all
        _ <- EitherT(sut.invalidateMemberWithToken(token)).leftWiden[AuthenticationError]
        tokensAfter = fetchTokens(store, Seq(p1))
      } yield {
        tokensBefore(p1) should have size 2
        tokensAfter shouldBe empty
      }
    }
  }
}
