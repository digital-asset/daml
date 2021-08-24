// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.daml.ledger.participant.state.v1
import com.google.rpc.error_details.ErrorInfo
import io.grpc.{Metadata, Status}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class AdaptedV1WriteServiceSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  "adaptSubmissionResult" should {
    "produce correct gRPC error codes for synchronous errors" in {
      val cases = Table(
        ("Submission Result", "Expected gRPC Error Code"),
        (v1.SubmissionResult.Overloaded, Status.RESOURCE_EXHAUSTED),
        (v1.SubmissionResult.NotSupported, Status.UNIMPLEMENTED),
        (v1.SubmissionResult.InternalError("some reason"), Status.INTERNAL),
        (
          v1.SubmissionResult.SynchronousReject(
            Status.UNAVAILABLE.asRuntimeException(new Metadata())
          ),
          Status.UNAVAILABLE,
        ),
      )

      forAll(cases) { case (input, expectedGrpcStatus) =>
        val SubmissionResult.SynchronousError(actual) =
          AdaptedV1WriteService.adaptSubmissionResult(input)
        actual.code should be(expectedGrpcStatus.getCode.value())
      }
    }

    "transform synchronous rejections' details into an ErrorInfo" in {
      val expectedMetadata = new Metadata()
      expectedMetadata.put(Metadata.Key.of("key", Metadata.ASCII_STRING_MARSHALLER), "value")
      val expectedStatus = Status.UNIMPLEMENTED.withDescription("not yet")
      val expectedStatusRuntimeException = expectedStatus.asRuntimeException(expectedMetadata)
      val input = v1.SubmissionResult.SynchronousReject(expectedStatusRuntimeException)

      val SubmissionResult.SynchronousError(actual) =
        AdaptedV1WriteService.adaptSubmissionResult(input)

      actual.code should be(expectedStatus.getCode.value())
      actual.details should have size 1
      val com.google.protobuf.any.Any(_, actualSerializedErrorInfo, _) = actual.details.head
      val actualErrorInfo = ErrorInfo.parseFrom(actualSerializedErrorInfo.newCodedInput())
      actualErrorInfo.reason should be(expectedStatusRuntimeException.getLocalizedMessage)
      actualErrorInfo.domain should be("Synchronous rejection")
      actualErrorInfo.metadata should be(Map("key" -> "value"))
    }

    "handle null metadata for synchronous rejection" in {
      val input =
        v1.SubmissionResult.SynchronousReject(Status.UNIMPLEMENTED.asRuntimeException(null))

      val SubmissionResult.SynchronousError(actual) =
        AdaptedV1WriteService.adaptSubmissionResult(input)

      actual.code should be(Status.UNIMPLEMENTED.getCode.value())
      actual.details should have size 1
      val com.google.protobuf.any.Any(_, actualSerializedErrorInfo, _) = actual.details.head
      val actualErrorInfo = ErrorInfo.parseFrom(actualSerializedErrorInfo.newCodedInput())
      actualErrorInfo.metadata should be(Map.empty)
    }
  }
}
