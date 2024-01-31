// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.logging.LoggingContextWithTrace
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AuthorizerSpec
    extends AsyncFlatSpec
    with BaseTest
    with Matchers
    with MockitoSugar
    with PekkoBeforeAndAfterAll {

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val className = classOf[Authorizer].getSimpleName
  private val dummyRequest = 1337L
  private val expectedSuccessfulResponse = "expectedSuccessfulResponse"
  private val dummyReqRes: Long => Future[String] =
    Map(dummyRequest -> Future.successful(expectedSuccessfulResponse))
  private val allAuthorized: (ClaimSet.Claims, Long) => Either[AuthorizationError, Long] =
    (_, req) => Right(req)
  private val unauthorized: (ClaimSet.Claims, Long) => Either[AuthorizationError, Long] = (_, _) =>
    Left(AuthorizationError.MissingAdminClaim)

  behavior of s"$className.authorize"

  it should "authorize if claims are valid" in {
    contextWithClaims {
      authorizer()
        .authorize(dummyReqRes)(allAuthorized)(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  behavior of s"$className.authorize"

  it should "return permission denied on authorization error" in {
    testPermissionDenied()
  }

  private def testPermissionDenied() =
    contextWithClaims {
      authorizer().authorize(dummyReqRes)(unauthorized)(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )

  private def assertExpectedFailure[T](
      expectedStatusCode: Status.Code
  ): Try[T] => Try[Assertion] = {
    case Failure(ex: StatusRuntimeException) =>
      ex.getStatus.getCode shouldBe expectedStatusCode
      Success(succeed)
    case ex => fail(s"Expected a failure with StatusRuntimeException but got $ex")
  }

  private def contextWithClaims[R](f: => R): R =
    io.grpc.Context.ROOT
      .withValue(AuthorizationInterceptor.contextKeyClaimSet, ClaimSet.Claims.Wildcard)
      .call(() => f)

  private def authorizer() = new Authorizer(
    () => Instant.ofEpochSecond(1337L),
    "some-ledger-id",
    "participant-id",
    mock[UserManagementStore],
    mock[ExecutionContext],
    userRightsCheckIntervalInSeconds = 1,
    pekkoScheduler = system.scheduler,
    telemetry = NoOpTelemetry,
    loggerFactory = loggerFactory,
  )
}
