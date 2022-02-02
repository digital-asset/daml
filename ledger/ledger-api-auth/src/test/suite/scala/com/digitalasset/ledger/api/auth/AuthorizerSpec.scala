// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.index.v2.UserManagementStore
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
      authorizer(selfServiceErrorCodes = false)
        .authorize(dummyReqRes)(allAuthorized)(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  behavior of s"$className.authorize (V1 error codes)"

  it should "return permission denied on authorization error" in {
    testPermissionDenied(selfServiceErrorCodes = false)
  }

  behavior of s"$className.authorize (V2 error codes)"

  it should "return permission denied on authorization error" in {
    testPermissionDenied(selfServiceErrorCodes = true)
  }

  private def testPermissionDenied(selfServiceErrorCodes: Boolean) =
    contextWithClaims {
      authorizer(selfServiceErrorCodes).authorize(dummyReqRes)(unauthorized)(dummyRequest)
    }
      .transform(
        assertExpectedFailure(selfServiceErrorCodes = selfServiceErrorCodes)(
          Status.PERMISSION_DENIED.getCode
        )
      )

  private def assertExpectedFailure[T](
      selfServiceErrorCodes: Boolean
  )(expectedStatusCode: Status.Code): Try[T] => Try[Assertion] = {
    case Failure(ex: StatusRuntimeException) =>
      ex.getStatus.getCode shouldBe expectedStatusCode
      if (selfServiceErrorCodes) {
        ex.getStatus.getDescription shouldBe "An error occurred. Please contact the operator and inquire about the request <no-correlation-id>"
      }
      Success(succeed)
    case ex => fail(s"Expected a failure with StatusRuntimeException but got $ex")
  }

  private def contextWithClaims[R](f: => R): R =
    io.grpc.Context.ROOT
      .withValue(AuthorizationInterceptor.contextKeyClaimSet, ClaimSet.Claims.Wildcard)
      .call(() => f)

  private def authorizer(selfServiceErrorCodes: Boolean) = Authorizer(
    () => Instant.ofEpochSecond(1337L),
    "some-ledger-id",
    "participant-id",
    new ErrorCodesVersionSwitcher(selfServiceErrorCodes),
    mock[UserManagementStore],
    mock[ExecutionContext],
    userRightsCheckIntervalInSeconds = 1,
    akkaScheduler = system.scheduler,
  )
}
