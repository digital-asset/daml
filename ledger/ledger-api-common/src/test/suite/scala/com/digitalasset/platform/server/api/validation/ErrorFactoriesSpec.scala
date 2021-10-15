// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.error.{
  DamlContextualizedErrorLogger,
  ContextualizedErrorLogger,
  ErrorCodesVersionSwitcher,
}
import com.daml.ledger.api.domain.LedgerId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.server.api.validation.ErrorFactories._
import com.google.rpc.Status
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class ErrorFactoriesSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  private val correlationId = "trace-id"
  private val logger = ContextualizedLogger.get(getClass)
  private val loggingContext = LoggingContext.ForTesting

  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, Some(correlationId))

  "ErrorFactories" should {
    "return the DuplicateCommandException" in {
      assertVersionedError(_.duplicateCommandException)(
        v1_code = Code.ALREADY_EXISTS,
        v1_message = "Duplicate command",
        v1_details = Seq(definiteAnswers(false)),
        v2_code = Code.ALREADY_EXISTS,
        v2_message =
          s"DUPLICATE_COMMAND(10,$correlationId): A command with the given command id has already been successfully processed",
      )
    }

    "return a permissionDenied error" in {
      assertVersionedError(_.permissionDenied())(
        v1_code = Code.PERMISSION_DENIED,
        v1_message = "",
        v1_details = Seq.empty,
        v2_code = Code.PERMISSION_DENIED,
        v2_message =
          s"An error occurred. Please contact the operator and inquire about the request $correlationId",
      )
    }

    "return a missingLedgerConfig error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        assertVersionedError(_.missingLedgerConfig(definiteAnswer))(
          v1_code = Code.UNAVAILABLE,
          v1_message = "The ledger configuration is not available.",
          v1_details = expectedDetails,
          v2_code = Code.NOT_FOUND,
          v2_message =
            s"LEDGER_CONFIGURATION_NOT_FOUND(11,$correlationId): The ledger configuration is not available.",
        )
      }
    }

    "return an aborted error" in {
      // TODO error codes: This error code is not specific enough.
      //                   Break down into more specific errors.
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
        assertVersionedError(_.invalidField("my field", "my message", definiteAnswer))(
          v1_code = Code.INVALID_ARGUMENT,
          v1_message = "Invalid field my field: my message",
          v1_details = expectedDetails,
          v2_code = Code.INVALID_ARGUMENT,
          v2_message =
            s"INVALID_FIELD(8,$correlationId): The submitted command has a field with invalid value: Invalid field my field: my message",
        )
      }
    }

    "return an unauthenticated error" in {
      assertVersionedError(_.unauthenticated())(
        v1_code = Code.UNAUTHENTICATED,
        v1_message = "",
        v1_details = Seq.empty,
        v2_code = Code.UNAUTHENTICATED,
        v2_message =
          s"An error occurred. Please contact the operator and inquire about the request $correlationId",
      )
    }

    "return a ledgerIdMismatch error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        assertVersionedError(
          _.ledgerIdMismatch(LedgerId("expected"), LedgerId("received"), definiteAnswer)
        )(
          v1_code = Code.NOT_FOUND,
          v1_message = "Ledger ID 'received' not found. Actual Ledger ID is 'expected'.",
          v1_details = expectedDetails,
          v2_code = Code.NOT_FOUND,
          v2_message =
            s"LEDGER_ID_MISMATCH(11,$correlationId): Ledger ID 'received' not found. Actual Ledger ID is 'expected'.",
        )
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
      assertVersionedError(_.participantPrunedDataAccessed("my message"))(
        v1_code = Code.NOT_FOUND,
        v1_message = "my message",
        v1_details = Seq.empty,
        v2_code = Code.OUT_OF_RANGE,
        v2_message = s"PARTICIPANT_PRUNED_DATA_ACCESSED(12,$correlationId): my message",
      )
    }

    "return an offsetAfterLedgerEnd error" in {
      assertVersionedError(_.offsetAfterLedgerEnd("my message"))(
        v1_code = Code.OUT_OF_RANGE,
        v1_message = "my message",
        v1_details = Seq.empty,
        v2_code = Code.OUT_OF_RANGE,
        v2_message = s"REQUESTED_OFFSET_OUT_OF_RANGE(12,$correlationId): my message",
      )
    }

    "return a serviceNotRunning error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        assertVersionedError(_.serviceNotRunning(definiteAnswer))(
          v1_code = Code.UNAVAILABLE,
          v1_message = "Service has been shut down.",
          v1_details = expectedDetails,
          v2_code = Code.UNAVAILABLE,
          v2_message = s"SERVICE_NOT_RUNNING(1,$correlationId): Service has been shut down.",
        )
      }
    }

    "return a missingLedgerConfigUponRequest error" in {
      assertVersionedError(_.missingLedgerConfigUponRequest)(
        v1_code = Code.NOT_FOUND,
        v1_message = "The ledger configuration is not available.",
        v1_details = Seq.empty,
        v2_code = Code.NOT_FOUND,
        v2_message =
          s"LEDGER_CONFIGURATION_NOT_FOUND(11,$correlationId): The ledger configuration is not available.",
      )
    }

    "return a missingField error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        assertVersionedError(_.missingField("my field", definiteAnswer))(
          v1_code = Code.INVALID_ARGUMENT,
          v1_message = "Missing field: my field",
          v1_details = expectedDetails,
          v2_code = Code.INVALID_ARGUMENT,
          v2_message =
            s"MISSING_FIELD(8,$correlationId): The submitted command is missing a mandatory field: my field",
        )
      }
    }

    "return an invalidArgument error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        assertVersionedError(_.invalidArgument(definiteAnswer)("my message"))(
          v1_code = Code.INVALID_ARGUMENT,
          v1_message = "Invalid argument: my message",
          v1_details = expectedDetails,
          v2_code = Code.INVALID_ARGUMENT,
          v2_message =
            s"INVALID_ARGUMENT(8,$correlationId): The submitted command has invalid arguments: my message",
        )
      }
    }

    "should create an ApiException without the stack trace" in {
      val status = Status.newBuilder().setCode(Code.INTERNAL.value()).build()
      val exception = grpcError(status)
      exception.getStackTrace shouldBe Array.empty
    }
  }

  private def assertVersionedError(
      error: ErrorFactories => StatusRuntimeException
  )(v1_code: Code, v1_message: String, v1_details: Seq[Any], v2_code: Code, v2_message: String) = {
    val errorFactoriesV1 = ErrorFactories(new ErrorCodesVersionSwitcher(false))
    val errorFactoriesV2 = ErrorFactories(new ErrorCodesVersionSwitcher(true))
    assertV1Error(error(errorFactoriesV1))(v1_code, v1_message, v1_details)
    assertV2Error(error(errorFactoriesV2))(v2_code, v2_message)
  }

  private def assertV1Error(
      statusRuntimeException: StatusRuntimeException
  )(expectedCode: Code, expectedMessage: String, expectedDetails: Seq[Any]) = {
    val status = StatusProto.fromThrowable(statusRuntimeException)
    status.getCode shouldBe expectedCode.value()
    status.getMessage shouldBe expectedMessage
    status.getDetailsList.asScala shouldBe expectedDetails
  }

  private def assertV2Error(
      statusRuntimeException: StatusRuntimeException
  )(expectedCode: Code, expectedMessage: String) = {
    val status = StatusProto.fromThrowable(statusRuntimeException)
    status.getCode shouldBe expectedCode.value()
    status.getMessage shouldBe expectedMessage
    // TODO error codes: Assert error details
    // TODO error codes: Assert logging
  }
}
