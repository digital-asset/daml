// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.clock.AdjustableClock
import com.daml.error.ErrorsAssertions
import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.ledger.api.auth.AuthorizationError.Expired
import com.digitalasset.canton.ledger.error.groups.AuthorizationChecksErrors
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.logging.{ErrorLoggingContext, SuppressionRule}
import io.grpc.StatusRuntimeException
import io.grpc.stub.ServerCallStreamObserver
import org.apache.pekko.actor.{Cancellable, Scheduler}
import org.mockito.{ArgumentCaptor, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.event.Level.INFO

import java.time.{Clock, Duration, Instant, ZoneId}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration as SDuration, FiniteDuration}

class OngoingAuthorizationObserverSpec
    extends AsyncFlatSpec
    with BaseTest
    with Matchers
    with Eventually
    with IntegrationPatience
    with MockitoSugar
    with ArgumentMatchersSugar
    with ErrorsAssertions {

  private implicit val errorLogger: ErrorLoggingContext = ErrorLoggingContext(
    loggerFactory.getTracedLogger(getClass),
    loggerFactory.properties,
    traceContext,
  )

  it should "signal onError aborting the stream when user rights state hasn't been refreshed in a timely manner" in {

    val userRightsCheckIntervalInSeconds = 10
    val (clock, delegate, tested, cancellableMock, _) =
      createInfrastructure(None, None, userRightsCheckIntervalInSeconds, None)

    // After 20 seconds pass we expect onError to be called due to lack of user rights state refresh task inactivity
    tested.onNext(1)
    clock.fastForward(Duration.ofSeconds(2.toLong * userRightsCheckIntervalInSeconds - 1))
    tested.onNext(2)
    clock.fastForward(Duration.ofSeconds(2))
    // Next onNext detects the user rights state refresh task inactivity
    tested.onNext(3)

    val captor = ArgumentCaptor.forClass(classOf[StatusRuntimeException])
    val order = inOrder(delegate)
    order.verify(delegate, times(1)).onNext(1)
    order.verify(delegate, times(1)).onNext(2)
    order.verify(delegate, times(1)).onError(captor.capture())
    order.verifyNoMoreInteractions()
    // Scheduled task is cancelled
    verify(cancellableMock, times(1)).cancel()
    assertError(
      actual = captor.getValue,
      expected = AuthorizationChecksErrors.StaleUserManagementBasedStreamClaims
        .Reject()
        .asGrpcError,
    )

    // onError has already been called by tested implementation so subsequent onNext, onError and onComplete
    // must not be forwarded to the delegate observer
    tested.onNext(4)
    tested.onError(new RuntimeException)
    tested.onCompleted()
    verifyNoMoreInteractions(delegate)

    succeed
  }

  def nonAbortCheck(
      parameterName: String,
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      tokenExpiryGracePeriodForStreams: Option[Duration],
  ): Unit =
    it should s"not abort the stream when the token has expired but adding $parameterName overlaps verification time" in {
      val (clock, delegate, tested, _, _) =
        createInfrastructure(jwtTimestampLeeway, tokenExpiryGracePeriodForStreams)

      tested.onNext(1)
      clock.fastForward(Duration.ofSeconds(1))
      tested.onNext(2)

      val order = inOrder(delegate)
      order.verify(delegate, times(1)).onNext(1)
      order.verify(delegate, times(1)).onNext(2)
      order.verifyNoMoreInteractions()

      succeed
    }

  nonAbortCheck("leeway time", Some(JwtTimestampLeeway(default = Some(1))), None)
  nonAbortCheck("token grace period", None, Some(Duration.ofSeconds(1)))

  it should s"never abort the stream when the token has expired but added an infinite grace period" in {
    val clockJump: Int = 1000 * 10000
    val (clock, delegate, tested, _, _) =
      createInfrastructure(
        None,
        Some(NonNegativeDuration(SDuration.Inf).asJavaApproximation),
        clockJump,
      )

    tested.onNext(1)
    clock.fastForward(Duration.ofSeconds(clockJump.toLong))
    tested.onNext(2)

    val order = inOrder(delegate)
    order.verify(delegate, times(1)).onNext(1)
    order.verify(delegate, times(1)).onNext(2)
    order.verifyNoMoreInteractions()

    succeed
  }

  def abortCheck(
      parameterName: String,
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      tokenExpiryGracePeriodForStreams: Option[Duration],
  ): Unit =
    it should s"abort the stream when the token has expired with $parameterName" in {

      inside(createInfrastructure(jwtTimestampLeeway, tokenExpiryGracePeriodForStreams)) {
        case (clock, delegate, tested, cancellableMock, Some(expiration)) =>
          // After 2 seconds have passed we expect onError to be called due to invalid expiration claim
          tested.onNext(1)
          clock.fastForward(Duration.ofSeconds(2))
          // Next onNext detects the invalid expiration claim
          tested.onNext(2)

          val captor = ArgumentCaptor.forClass(classOf[StatusRuntimeException])
          val order = inOrder(delegate)
          order.verify(delegate, times(1)).onNext(1)
          order.verify(delegate, times(1)).onError(captor.capture())
          order.verifyNoMoreInteractions()

          loggerFactory.assertLogs(SuppressionRule.Level(INFO))(
            within = {
              // Scheduled task is cancelled
              verify(cancellableMock, times(1)).cancel()
              assertError(
                actual = captor.getValue,
                expected = AuthorizationChecksErrors.AccessTokenExpired
                  .Reject(Expired(expiration, clock.instant).reason)
                  .asGrpcError,
              )
            },
            assertions =
              _.infoMessage should include("ACCESS_TOKEN_EXPIRED(2,0): Claims were valid until "),
          )

          // onError has already been called by tested implementation so subsequent onNext, onError and onComplete
          // must not be forwarded to the delegate observer
          tested.onNext(3)
          tested.onError(new RuntimeException)
          tested.onCompleted()
          verifyNoMoreInteractions(delegate)

          succeed
      }
    }

  abortCheck("leeway time", Some(JwtTimestampLeeway(default = Some(1))), None)
  abortCheck("token grace period", None, Some(Duration.ofSeconds(1)))

  def createInfrastructure(
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      tokenExpiryGracePeriodForStreams: Option[Duration],
      userRightsCheckIntervalInSeconds: Int = 10,
      getExpiry: Option[AdjustableClock => Instant] = Some(_.instant.plusSeconds(1)),
  ): (
      AdjustableClock,
      ServerCallStreamObserver[Int],
      ServerCallStreamObserver[Int],
      Cancellable,
      Option[Instant],
  ) = {
    val clock = AdjustableClock(
      baseClock = Clock.fixed(Instant.now(), ZoneId.systemDefault()),
      offset = Duration.ZERO,
    )
    val delegate = mock[ServerCallStreamObserver[Int]]
    val mockScheduler = mock[Scheduler]
    // Set scheduler to do nothing
    val cancellableMock = mock[Cancellable]
    when(
      mockScheduler.scheduleWithFixedDelay(any[FiniteDuration], any[FiniteDuration])(
        any[Runnable]
      )(
        any[ExecutionContext]
      )
    ).thenReturn(cancellableMock)
    val expiration = getExpiry.map(_.apply(clock))
    val tested = OngoingAuthorizationObserver(
      observer = delegate,
      originalClaims = ClaimSet.Claims.Empty.copy(
        resolvedFromUser = true,
        applicationId = Some("some_user_id"),
        // the expiration claim will be invalid in the next second
        expiration = expiration,
      ),
      nowF = () => clock.instant,
      userManagementStore = mock[UserManagementStore],
      userRightsCheckIntervalInSeconds = userRightsCheckIntervalInSeconds,
      pekkoScheduler = mockScheduler,
      jwtTimestampLeeway = jwtTimestampLeeway,
      tokenExpiryGracePeriodForStreams = tokenExpiryGracePeriodForStreams,
      loggerFactory = loggerFactory,
    )(executionContext, traceContext)
    (clock, delegate, tested, cancellableMock, expiration)
  }
}
