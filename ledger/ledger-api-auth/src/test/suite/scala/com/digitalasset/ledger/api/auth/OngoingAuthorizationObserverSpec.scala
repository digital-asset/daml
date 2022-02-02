// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.{Clock, Duration, Instant, ZoneId}

import akka.actor.{Cancellable, Scheduler}
import com.daml.clock.AdjustableClock
import com.daml.error.ErrorsAssertions
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.logging.LoggingContext
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.StatusRuntimeException
import io.grpc.stub.ServerCallStreamObserver
import org.mockito.{ArgumentCaptor, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class OngoingAuthorizationObserverSpec
    extends AsyncFlatSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with ErrorsAssertions {

  private val loggingContext = LoggingContext.ForTesting

  it should "signal onError aborting the stream when user rights state hasn't been refreshed in a timely manner" in {
    val clock = AdjustableClock(
      baseClock = Clock.fixed(Instant.now(), ZoneId.systemDefault()),
      offset = Duration.ZERO,
    )
    val delegate = mock[ServerCallStreamObserver[Int]]
    val mockScheduler = mock[Scheduler]
    // Set scheduler to do nothing
    val cancellableMock = mock[Cancellable]
    when(
      mockScheduler.scheduleWithFixedDelay(any[FiniteDuration], any[FiniteDuration])(any[Runnable])(
        any[ExecutionContext]
      )
    ).thenReturn(cancellableMock)
    val userRightsCheckIntervalInSeconds = 10
    val tested = new OngoingAuthorizationObserver(
      observer = delegate,
      originalClaims = ClaimSet.Claims.Empty.copy(resolvedFromUser = true),
      nowF = clock.instant,
      errorFactories = mock[ErrorFactories],
      userManagementStore = mock[UserManagementStore],
      // This is also the user rights state refresh timeout
      userRightsCheckIntervalInSeconds = userRightsCheckIntervalInSeconds,
      akkaScheduler = mockScheduler,
    )(loggingContext, executionContext)

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
      expectedF = LedgerApiErrors.AuthorizationChecks.StaleUserManagementBasedStreamClaims
        .Reject()(_)
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

}
