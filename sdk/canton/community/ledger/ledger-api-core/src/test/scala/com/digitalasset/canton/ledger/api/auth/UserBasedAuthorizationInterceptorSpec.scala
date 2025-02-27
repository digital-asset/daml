// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.auth.{AuthService, AuthorizationInterceptor, ClaimSet}
import com.digitalasset.canton.ledger.api.auth.interceptor.UserBasedAuthorizationInterceptor
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.protobuf.StatusProto
import io.grpc.{Metadata, ServerCall, Status}
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.event.Level

import scala.concurrent.{Future, Promise}
import scala.util.Success

class UserBasedAuthorizationInterceptorSpec
    extends AsyncFlatSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with BaseTest
    with HasExecutionContext {

  private val className = classOf[AuthorizationInterceptor].getSimpleName

  behavior of s"$className.interceptCall"

  private val AuthorizationInterceptorSuppressionRule: SuppressionRule =
    SuppressionRule.forLogger[AuthorizationInterceptor] && SuppressionRule.Level(Level.ERROR)

  it should "close the ServerCall with a V2 status code on decoding failure" in {
    loggerFactory.assertLogs(AuthorizationInterceptorSuppressionRule)(
      within = testServerCloseError { case (actualStatus, actualMetadata) =>
        actualStatus.getCode shouldBe Status.Code.INTERNAL
        actualStatus.getDescription shouldBe "An error occurred. Please contact the operator and inquire about the request <no-correlation-id> with tid <no-tid>"

        val actualRpcStatus = StatusProto.fromStatusAndTrailers(actualStatus, actualMetadata)
        actualRpcStatus.getDetailsList.size() shouldBe 0
      },
      assertions = _.errorMessage should include(
        "INTERNAL_AUTHORIZATION_ERROR(4,0): Failed to get claims from request metadata"
      ),
    )
  }

  private def testServerCloseError(
      assertRpcStatus: (Status, Metadata) => Assertion
  ): Future[Assertion] = {
    val authService = mock[AuthService]
    val identityProviderAwareAuthService = mock[AuthService]
    val userManagementService = mock[UserManagementStore]
    val serverCall = mock[ServerCall[Nothing, Nothing]]
    val failedMetadataDecode =
      Future.failed[ClaimSet](new RuntimeException("some internal failure"))

    val promise = Promise[Unit]()
    // Using a promise to ensure the verify call below happens after the expected call to `serverCall.close`
    when(serverCall.close(any[Status], any[Metadata])).thenAnswer {
      promise.complete(Success(()))
      ()
    }

    val authorizationInterceptor =
      new UserBasedAuthorizationInterceptor(
        List(authService, identityProviderAwareAuthService),
        Some(userManagementService),
        NoOpTelemetry,
        loggerFactory,
        executionContext,
      )

    val statusCaptor = ArgCaptor[Status]
    val metadataCaptor = ArgCaptor[Metadata]

    when(
      identityProviderAwareAuthService.decodeMetadata(any[Metadata])(any[TraceContext])
    )
      .thenReturn(Future.successful(ClaimSet.Unauthenticated))
    when(authService.decodeMetadata(any[Metadata])(anyTraceContext))
      .thenReturn(failedMetadataDecode)
    authorizationInterceptor.interceptCall[Nothing, Nothing](serverCall, new Metadata(), null)

    promise.future.map { _ =>
      verify(serverCall).close(statusCaptor.capture, metadataCaptor.capture)
      assertRpcStatus(statusCaptor.value, metadataCaptor.value)
    }
  }

}
