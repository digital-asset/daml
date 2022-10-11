// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.UserManagementStore
import org.mockito.MockitoSugar

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AuthorizerSpec
    extends AsyncFlatSpec
    with Matchers
    with MockitoSugar
    with AkkaBeforeAndAfterAll {
  private val className = classOf[Authorizer].getSimpleName
  private val dummyRequest = 1337L
  private val expectedSuccessfulResponse = "expectedSuccessfulResponse"
  private val dummyReqRes: Long => Future[String] =
    Map(dummyRequest -> Future.successful(expectedSuccessfulResponse))
  private val allAuthorized: ClaimSet.Claims => Either[AuthorizationError, Unit] = _ => Right(())
  private val unauthorized: ClaimSet.Claims => Either[AuthorizationError, Unit] = _ =>
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
    akkaScheduler = system.scheduler,
  )(LoggingContext.ForTesting)
}
