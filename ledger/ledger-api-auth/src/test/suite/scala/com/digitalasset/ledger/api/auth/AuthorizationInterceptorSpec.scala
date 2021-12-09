// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import io.grpc.protobuf.StatusProto
import io.grpc.{Metadata, ServerCall, Status}
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext.global
import scala.concurrent.Promise
import scala.util.Success

class AuthorizationInterceptorSpec
    extends AsyncFlatSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar {
  private val className = classOf[AuthorizationInterceptor].getSimpleName

  behavior of s"$className.interceptCall"

  it should "close the ServerCall with a V1 status code on decoding failure" in {
    testServerCloseError(usesSelfServiceErrorCodes = false) { case (actualStatus, actualMetadata) =>
      actualStatus.getCode shouldBe Status.Code.INTERNAL
      actualStatus.getDescription shouldBe "Failed to get claims from request metadata"
      actualMetadata.keys() shouldBe empty
    }
  }

  it should "close the ServerCall with a V2 status code on decoding failure" in {
    testServerCloseError(usesSelfServiceErrorCodes = true) { case (actualStatus, actualMetadata) =>
      actualStatus.getCode shouldBe Status.Code.INTERNAL
      actualStatus.getDescription shouldBe "An error occurred. Please contact the operator and inquire about the request <no-correlation-id>"

      val actualRpcStatus = StatusProto.fromStatusAndTrailers(actualStatus, actualMetadata)
      actualRpcStatus.getDetailsList.size() shouldBe 0
    }
  }

  private def testServerCloseError(
      usesSelfServiceErrorCodes: Boolean
  )(assertRpcStatus: (Status, Metadata) => Assertion) = {
    val authService = mock[AuthService]
    val serverCall = mock[ServerCall[Nothing, Nothing]]
    val failedMetadataDecode = CompletableFuture.supplyAsync[ClaimSet](() =>
      throw new RuntimeException("some internal failure")
    )

    val promise = Promise[Unit]()
    // Using a promise to ensure the verify call below happens after the expected call to `serverCall.close`
    when(serverCall.close(any[Status], any[Metadata])).thenAnswer {
      promise.complete(Success(()))
      ()
    }

    val errorCodesStatusSwitcher = new ErrorCodesVersionSwitcher(usesSelfServiceErrorCodes)
    val authorizationInterceptor =
      AuthorizationInterceptor(authService, global, errorCodesStatusSwitcher)

    val statusCaptor = ArgCaptor[Status]
    val metadataCaptor = ArgCaptor[Metadata]

    when(authService.decodeMetadata(any[Metadata])).thenReturn(failedMetadataDecode)
    authorizationInterceptor.interceptCall[Nothing, Nothing](serverCall, new Metadata(), null)

    promise.future.map { _ =>
      verify(serverCall).close(statusCaptor.capture, metadataCaptor.capture)
      assertRpcStatus(statusCaptor.value, metadataCaptor.value)
    }
  }
}
