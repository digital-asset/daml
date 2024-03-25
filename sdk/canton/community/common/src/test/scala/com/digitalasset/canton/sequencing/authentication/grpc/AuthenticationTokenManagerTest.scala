// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication.grpc

import cats.data.EitherT
import cats.implicits.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
}
import com.digitalasset.canton.time.Clock
import io.grpc.Status
import org.mockito.ArgumentMatchersSugar
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{Future, Promise}

object AuthenticationTokenManagerTest extends org.mockito.MockitoSugar with ArgumentMatchersSugar {
  val mockClock = mock[Clock]
  when(mockClock.scheduleAt(any[CantonTimestamp => Unit], any[CantonTimestamp]))
    .thenReturn(FutureUnlessShutdown.unit)
}

class AuthenticationTokenManagerTest extends AsyncWordSpec with BaseTest {

  val crypto = new SymbolicPureCrypto
  val token1: AuthenticationToken = AuthenticationToken.generate(crypto)
  val token2: AuthenticationToken = AuthenticationToken.generate(crypto)
  val now = CantonTimestamp.Epoch

  "first call to getToken will obtain it" in {
    val (tokenManager, mock, _) = setup()

    mock.succeed(token1)

    for {
      _ <- tokenManager.getToken
    } yield mock.callCount shouldBe 1
  }

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
  }

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
  }

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
  }

  "invalidateToken will cause obtain to be called on next call" in {
    val (tokenManager, mock, _) = setup()

    mock.succeed(token1)

    for {
      result1 <- tokenManager.getToken.value.map(_.value)
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
  }

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
  }

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
    clue("succeed with token1") { obtainMock.succeed(token1) }

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

  }

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
    private val nextResult = new AtomicReference[Promise[Either[Status, AuthenticationToken]]]()

    resetNextResult()

    def callCount: Int = callCounter.get()

    def obtain(): EitherT[Future, Status, AuthenticationTokenWithExpiry] = {
      callCounter.incrementAndGet()
      EitherT(
        nextResult.get.future.map(
          _.map(token => AuthenticationTokenWithExpiry(token, now.plusSeconds(100)))
        )
      )
    }

    def resetNextResult(): Unit = {
      nextResult.set(Promise[Either[Status, AuthenticationToken]]())
    }

    def succeed(token: AuthenticationToken): Unit = {
      nextResult.get().success(Right(token))
    }

    def error(message: String): Unit = {
      nextResult.get().success(Left(Status.PERMISSION_DENIED.withDescription(message)))
    }

    def fail(throwable: Throwable): Unit = {
      nextResult.get().failure(throwable)
    }
  }
}
