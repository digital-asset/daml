// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.google.rpc.ErrorInfo
import io.grpc.{Metadata, ServerCall, Status}
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext.global
import io.grpc.protobuf.StatusProto

class AuthorizationInterceptorSpec
    extends AnyFlatSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar {
  private val className = classOf[AuthorizationInterceptor].getSimpleName

  behavior of s"$className.interceptCall"

  it should "close the ServerCall with a V1 status code on decoding failure" in {
    testServerCloseError(usesSelfServiceErrorCodes = false) { case (status, metadata) =>
      status.getCode shouldBe Status.Code.INTERNAL
      status.getDescription shouldBe "Failed to get claims from request metadata"
      metadata.keys() shouldBe empty
    }
  }

  it should "close the ServerCall with a V2 status code on decoding failure" in {
    testServerCloseError(usesSelfServiceErrorCodes = true) { case (actualStatus, actualTrailers) =>
      actualStatus.getCode shouldBe Status.Code.INTERNAL
      actualStatus.getDescription shouldBe "An error occurred. Please contact the operator and inquire about the request <no-correlation-id>"

      val actualRpcStatus = StatusProto.fromStatusAndTrailers(actualStatus, actualTrailers)
      actualRpcStatus.getDetailsList.size() shouldBe 1
      val errorInfo = actualRpcStatus.getDetailsList.get(0).unpack(classOf[ErrorInfo])
      errorInfo.getReason shouldBe "INTERNAL_AUTHORIZATION_ERROR"
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

    val errorCodesStatusSwitcher = new ErrorCodesVersionSwitcher(usesSelfServiceErrorCodes)
    val authorizationInterceptor =
      new AuthorizationInterceptor(authService, global, errorCodesStatusSwitcher)

    val statusCaptor = ArgCaptor[Status]
    val metadataCaptor = ArgCaptor[Metadata]

    when(authService.decodeMetadata(any[Metadata])).thenReturn(failedMetadataDecode)
    authorizationInterceptor.interceptCall[Nothing, Nothing](serverCall, new Metadata(), null)

    verify(serverCall, timeout(1000)).close(statusCaptor.capture, metadataCaptor.capture)

    val actualStatus = statusCaptor.value

    assertRpcStatus(actualStatus, metadataCaptor.value)
  }
}
