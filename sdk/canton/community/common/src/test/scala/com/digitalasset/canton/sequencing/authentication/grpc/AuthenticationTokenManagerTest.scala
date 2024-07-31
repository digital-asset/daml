// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication.grpc

import cats.data.EitherT
import cats.implicits.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.Status
import org.mockito.ArgumentMatchersSugar
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

object AuthenticationTokenManagerTest extends org.mockito.MockitoSugar with ArgumentMatchersSugar {
  val mockClock: Clock = mock[Clock]
  when(mockClock.scheduleAt(any[CantonTimestamp => Unit], any[CantonTimestamp]))
    .thenReturn(FutureUnlessShutdown.unit)
}

class AuthenticationTokenManagerTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val crypto = new SymbolicPureCrypto
  private val token1 = AuthenticationToken.generate(crypto)
  private val token2 = AuthenticationToken.generate(crypto)
  private val now = CantonTimestamp.Epoch

  "first call to getToken will obtain it" in {
    val (tokenManager, mock, _) = setup()

    mock.succeed(token1)

    for {
      _ <- tokenManager.getToken
    } yield mock.callCount shouldBe 1
  }.failOnShutdown.futureValue

  "multiple calls to getToken before obtain has completed will return pending" in {
    val (tokenManager, mock, _) = setup()

    val call1 = tokenManager.getToken
    val call2 = tokenManager.getToken

    mock.succeed(token1)

    for {
      result1 <- call1.value.map(_.value)
      result2 <- call2.value.map(_.value)
    } yield {
      mock.callCount shouldBe 1
      result1 shouldEqual result2
    }
  }.failOnShutdown.futureValue

  "getToken after error will cause refresh" in {
    val (tokenManager, mock, _) = setup()

    for {
      error1 <- loggerFactory.suppressWarningsAndErrors {
        val call1 = tokenManager.getToken
        mock.error("uh oh")
        call1.value.map(_.left.value)
      }
      _ = error1.getDescription shouldBe "uh oh"
      _ = mock.resetNextResult()
      call2 = tokenManager.getToken
      _ = mock.succeed(token1)
      result2 <- call2.value.map(_.value)
    } yield {
      result2 shouldBe token1
    }
  }.failOnShutdown.futureValue

  "invalidateToken will cause obtain to be called on next call" in {
    val (tokenManager, mock, _) = setup()

    mock.succeed(token1)

    for {
      result1 <- tokenManager.getToken
      _ = {
        mock.resetNextResult()
        tokenManager.invalidateToken(result1)
        mock.succeed(token2)
      }
      result2 <- tokenManager.getToken
    } yield {
      result1 shouldBe token1
      result2 shouldBe token2
      mock.callCount shouldBe 2
    }
  }.failOnShutdown.futureValue

  "invalidateToken with a different token wont cause a refresh" in {
    val (tokenManager, mock, _) = setup()

    mock.succeed(token1)

    for {
      result1 <- tokenManager.getToken.value.map(_.value)
      _ = {
        tokenManager.invalidateToken(token2)
      }
      result2 <- tokenManager.getToken.value.map(_.value)
    } yield {
      result1 shouldBe result2
      mock.callCount shouldBe 1 // despite invalidation
    }
  }.failOnShutdown.futureValue

  "automatically renew token in due time" in {
    val clockMock = mock[Clock]
    val retryMe = new AtomicReference[Option[CantonTimestamp => Unit]](None)
    when(clockMock.scheduleAt(any[CantonTimestamp => Unit], any[CantonTimestamp]))
      .thenAnswer[CantonTimestamp => Unit, CantonTimestamp] { case (action, _) =>
        retryMe.getAndUpdate(_ => Some(action)) shouldBe empty
        FutureUnlessShutdown.unit
      }

    val (tokenManager, obtainMock, _) = setup(Some(clockMock))
    val call1 = clue("get token1") {
      tokenManager.getToken
    }
    clue("succeed with token1")(obtainMock.succeed(token1))

    for {
      // wait for token to succeed
      t1 <- call1
      _ = retryMe.get() should not be empty
      _ = obtainMock.resetNextResult()
      // now, invoke the scheduled renewal
      _ = retryMe.get().value.apply(CantonTimestamp.Epoch)
      // obtain intermediate result
      t2f = tokenManager.getToken
      // satisfy this request
      _ = obtainMock.succeed(token2)
      t3 <- tokenManager.getToken
      t2 <- t2f
    } yield {
      t1 shouldBe token1
      t2 shouldBe token2
      t3 shouldBe token2
    }
  }.failOnShutdown.futureValue

  "getToken after failure will cause refresh" in {
    val (tokenManager, mock, _) = setup()

    for {
      error1 <- loggerFactory.suppressWarningsAndErrors {
        val call1 = tokenManager.getToken
        mock.fail(new RuntimeException("uh oh"))

        call1.value.failed.map(_.getMessage)
      }
      _ = error1 shouldBe "uh oh"
      _ = mock.resetNextResult()
      call2 = tokenManager.getToken
      _ = mock.succeed(token1)
      result2 <- call2.value.map(_.value)
    } yield result2 shouldBe token1
  }.failOnShutdown.futureValue

  private def setup(
      clockO: Option[Clock] = None
  ): (AuthenticationTokenManager, ObtainTokenMock, Clock) = {
    val mck = new ObtainTokenMock
    val clock = clockO.getOrElse(AuthenticationTokenManagerTest.mockClock)
    val tokenManager = new AuthenticationTokenManager(
      _ => mck.obtain(),
      false,
      AuthenticationTokenManagerConfig(),
      clock,
      loggerFactory,
    )
    (tokenManager, mck, clock)
  }

  private class ObtainTokenMock {
    private val callCounter = new AtomicInteger()
    private val nextResult =
      new AtomicReference[PromiseUnlessShutdown[Either[Status, AuthenticationToken]]]()

    resetNextResult()

    def callCount: Int = callCounter.get()

    def obtain(): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] = {
      callCounter.incrementAndGet()
      EitherT(
        nextResult.get.futureUS.map(
          _.map(token => AuthenticationTokenWithExpiry(token, now.plusSeconds(100)))
        )
      )
    }

    def resetNextResult(): Unit =
      nextResult.set(
        new PromiseUnlessShutdown[Either[Status, AuthenticationToken]]("test", futureSupervisor)
      )

    def succeed(token: AuthenticationToken): Unit =
      nextResult.get().success(UnlessShutdown.Outcome(Right(token)))

    def error(message: String): Unit =
      nextResult
        .get()
        .success(UnlessShutdown.Outcome(Left(Status.PERMISSION_DENIED.withDescription(message))))

    def fail(throwable: Throwable): Unit =
      nextResult.get().failure(throwable)
  }
}
