// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.grpc

import com.daml.ledger.grpc.GrpcStatuses.{CompletionOffsetKey, DefiniteAnswerKey}
import com.daml.ledger.offset.Offset
import com.google.protobuf.any
import com.google.rpc.error_details.{ErrorInfo, RequestInfo}
import com.google.rpc.status.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.wordspec.AnyWordSpec

class GrpcStatusesSpec extends AnyWordSpec with Matchers {
  "isDefiniteAnswer" should {
    "return correct value" in {
      val anErrorInfo = ErrorInfo.of("reason", "domain", Map.empty)
      val testCases = Table(
        ("Description", "Error Info", "Expected"),
        (
          "ErrorInfo contains definite answer key and its value is true",
          Some(anErrorInfo.copy(metadata = Map(DefiniteAnswerKey -> "true"))),
          true,
        ),
        (
          "ErrorInfo contains definite answer key and its value is false",
          Some(anErrorInfo.copy(metadata = Map(DefiniteAnswerKey -> "false"))),
          false,
        ),
        (
          "ignore casing of value associated to definite answer key (#1)",
          Some(anErrorInfo.copy(metadata = Map(DefiniteAnswerKey -> "TRUE"))),
          true,
        ),
        (
          "ignore casing of value associated to definite answer key (#2)",
          Some(anErrorInfo.copy(metadata = Map(DefiniteAnswerKey -> "True"))),
          true,
        ),
        (
          "ErrorInfo does not contain definite answer key",
          Some(anErrorInfo.copy(metadata = Map("some" -> "key"))),
          false,
        ),
        ("no ErrorInfo is available", None, false),
      )

      forAll(testCases) { case (_, errorInfoMaybe, expected) =>
        val details =
          errorInfoMaybe.map(errorInfo => any.Any.pack(errorInfo)).map(Seq(_)).getOrElse(Seq.empty)
        val inputStatus = Status.of(123, "an error", details)
        GrpcStatuses.isDefiniteAnswer(inputStatus) should be(expected)
      }
    }
  }

  "completeWithOffset" should {
    "throw in case no error details can be found" in {
      assertThrows[IllegalArgumentException] {
        GrpcStatuses.completeWithOffset(Status.defaultInstance, aCompletionOffset)
      }
    }

    "throw in case no ErrorInfo message is available" in {
      val aMessage = RequestInfo.of("a", "b")
      val inputStatus = Status.of(123, "an error", Seq(any.Any.pack(aMessage)))
      assertThrows[IllegalArgumentException] {
        GrpcStatuses.completeWithOffset(inputStatus, aCompletionOffset)
      }
    }

    "update metadata for ErrorInfo with completion offset at completion key" in {
      val anErrorInfo = ErrorInfo.of("reason", "domain", Map("key" -> "value"))
      val inputStatus = Status.of(123, "an error", Seq(any.Any.pack(anErrorInfo)))

      GrpcStatuses.completeWithOffset(inputStatus, aCompletionOffset) should be(
        inputStatus.copy(details =
          Seq(
            any.Any.pack(
              anErrorInfo
                .copy(metadata =
                  anErrorInfo.metadata + (CompletionOffsetKey -> aCompletionOffset.toHexString)
                )
            )
          )
        )
      )
    }
  }

  private lazy val aCompletionOffset = Offset.fromByteArray(Array[Byte](1, 2))
}
