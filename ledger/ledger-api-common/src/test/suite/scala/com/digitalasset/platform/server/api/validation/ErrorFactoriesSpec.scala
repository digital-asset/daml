// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.api.domain.LedgerId
import com.daml.platform.server.api.validation.ErrorFactories._
import io.grpc.Status.Code
import io.grpc.protobuf.StatusProto
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class ErrorFactoriesSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "ErrorFactories" should {
    "return the DuplicateCommandException" in {
      val status = StatusProto.fromThrowable(duplicateCommandException())
      status.getCode shouldBe Code.ALREADY_EXISTS.value()
      status.getMessage shouldBe "Duplicate command"
      status.getDetailsList.asScala shouldBe Seq(definiteAnswers(false))
    }

    "return a permissionDenied error" in {
      permissionDenied().getStatus.getCode shouldBe Code.PERMISSION_DENIED
    }

    "return a notFound error" in {
      val exception = notFound("my message").getStatus
      exception.getCode shouldBe Code.NOT_FOUND
      exception.getDescription shouldBe "my message"
    }

    "return a missingLedgerConfig error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val exception = missingLedgerConfig(definiteAnswer)
        val status = StatusProto.fromThrowable(exception)
        status.getCode shouldBe Code.UNAVAILABLE.value()
        status.getDetailsList.asScala shouldBe expectedDetails
      }
    }

    "return an aborted error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val exception = aborted("my message", definiteAnswer)
        val status = StatusProto.fromThrowable(exception)
        status.getCode shouldBe Code.ABORTED.value()
        status.getMessage shouldBe "my message"
        status.getDetailsList.asScala shouldBe expectedDetails
      }
    }

    "return an invalidField error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val exception = invalidField("my field", "my message", definiteAnswer)
        val status = StatusProto.fromThrowable(exception)
        status.getCode shouldBe Code.INVALID_ARGUMENT.value()
        status.getMessage shouldBe "Invalid field my field: my message"
        status.getDetailsList.asScala shouldBe expectedDetails
      }
    }

    "return an unauthenticated error" in {
      unauthenticated().getStatus.getCode shouldBe Code.UNAUTHENTICATED
    }

    "return a ledgerIdMismatch error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val exception = ledgerIdMismatch(LedgerId("expected"), LedgerId("received"), definiteAnswer)
        val status = StatusProto.fromThrowable(exception)
        status.getCode shouldBe Code.NOT_FOUND.value()
        status.getMessage shouldBe "Ledger ID 'received' not found. Actual Ledger ID is 'expected'."
        status.getDetailsList.asScala shouldBe expectedDetails
      }
    }

    "fail on creating a ledgerIdMismatch error due to a wrong definite answer" in {
      an[IllegalArgumentException] should be thrownBy ledgerIdMismatch(
        LedgerId("expected"),
        LedgerId("received"),
        definiteAnswer = Some(true),
      )
    }

    "return a participantPrunedDataAccessed error" in {
      val exception = participantPrunedDataAccessed("my message").getStatus
      exception.getCode shouldBe Code.NOT_FOUND
      exception.getDescription shouldBe "my message"
    }

    "return an outOfRange error" in {
      val exception = outOfRange("my message").getStatus
      exception.getCode shouldBe Code.OUT_OF_RANGE
      exception.getDescription shouldBe "my message"
    }

    "return a serviceNotRunning error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val exception = serviceNotRunning(definiteAnswer)
        val status = StatusProto.fromThrowable(exception)
        status.getCode shouldBe Code.UNAVAILABLE.value()
        status.getMessage shouldBe "Service has been shut down."
        status.getDetailsList.asScala shouldBe expectedDetails
      }
    }

    "return a missingLedgerConfigUponRequest error" in {
      val exception = missingLedgerConfigUponRequest().getStatus
      exception.getCode shouldBe Code.NOT_FOUND
      exception.getDescription shouldBe "The ledger configuration is not available."
    }

    "return a missingField error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val exception = missingField("my field", definiteAnswer)
        val status = StatusProto.fromThrowable(exception)
        status.getCode shouldBe Code.INVALID_ARGUMENT.value()
        status.getMessage shouldBe "Missing field: my field"
        status.getDetailsList.asScala shouldBe expectedDetails
      }
    }

    "return an invalidArgument error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val exception = invalidArgument(definiteAnswer)("my message")
        val status = StatusProto.fromThrowable(exception)
        status.getCode shouldBe Code.INVALID_ARGUMENT.value()
        status.getMessage shouldBe "Invalid argument: my message"
        status.getDetailsList.asScala shouldBe expectedDetails
      }
    }
  }
}
