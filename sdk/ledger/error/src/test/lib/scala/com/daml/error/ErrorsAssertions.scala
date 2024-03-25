// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.utils.ErrorDetails
import com.daml.error.utils.ErrorDetails.ErrorInfoDetail
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import LogCollector.ExpectedLogEntry
import com.daml.scalautil.Statement
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.scalatest.{AppendedClues, Assertion, OptionValues}
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import org.scalatest.Checkpoints.Checkpoint

trait ErrorsAssertions extends Matchers with OptionValues with AppendedClues {

  private val logger = ContextualizedLogger.get(getClass)
  private val loggingContext = LoggingContext.ForTesting
  private val errorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)

  /** NOTE: This method is not suitable for:
    * 1) security sensitive error codes (e.g. internal or authentication related) as they are stripped from all the details when being converted to instances of [[StatusRuntimeException]],
    * 2) error codes that do not translate to gRPC level errors (i.e. error codes that don't have a corresponding gRPC status)
    */
  def assertMatchesErrorCode(
      actual: StatusRuntimeException,
      expectedErrorCode: ErrorCode,
  ): Assertion = {
    val actualErrorCodeId = ErrorDetails.from(actual).collectFirst {
      case ErrorInfoDetail(errorCodeId, _) => errorCodeId
    }
    val actualDescription = Option(actual.getStatus.getDescription)
    val actualStatusCode = actual.getStatus.getCode
    val cp = new Checkpoint
    cp { Statement.discard { actualErrorCodeId.value shouldBe expectedErrorCode.id } }
    cp { Statement.discard { Some(actualStatusCode) shouldBe expectedErrorCode.category.grpcCode } }
    cp { Statement.discard { actualDescription.value should startWith(expectedErrorCode.id) } }
    cp.reportAll()
    succeed
  }

  def assertStatus(
      actual: com.google.rpc.Status,
      expected: com.google.rpc.Status,
  ): Assertion = {
    val actualDetails = ErrorDetails.from(actual)
    val expectedDetails = ErrorDetails.from(expected)
    val actualDescription = Option(actual.getMessage)
    val expectedDescription = Option(expected.getMessage)
    val actualStatusCode = actual.getCode
    val expectedStatusCode = expected.getCode
    val cp = new Checkpoint
    cp { Statement.discard { actualDescription shouldBe expectedDescription } }
    cp {
      Statement.discard {
        actualStatusCode shouldBe expectedStatusCode withClue (s", expecting status code: '${expectedStatusCode}''")
      }
    }
    cp { Statement.discard { actualDetails should contain theSameElementsAs expectedDetails } }
    cp.reportAll()
    succeed
  }

  def assertError(
      actual: StatusRuntimeException,
      expectedF: ContextualizedErrorLogger => StatusRuntimeException,
  ): Unit = {
    assertError(
      actual = actual,
      expected = expectedF(errorLogger),
    )
  }

  /** Asserts that the two errors have the same code, message and details.
    */
  def assertError(
      actual: StatusRuntimeException,
      expected: StatusRuntimeException,
  ): Unit = {
    val expectedStatus = StatusProto.fromThrowable(expected)
    val expectedDetails = expectedStatus.getDetailsList.asScala.toSeq
    assertError(
      actual = actual,
      expectedStatusCode = expected.getStatus.getCode,
      expectedMessage = expectedStatus.getMessage,
      expectedDetails = ErrorDetails.from(expectedDetails),
    )
  }

  /** @param verifyEmptyStackTrace - should be enabled for the server-side testing and disabled for the client side testing
    */
  def assertError(
      actual: StatusRuntimeException,
      expectedStatusCode: Code,
      expectedMessage: String,
      expectedDetails: Seq[ErrorDetails.ErrorDetail],
      verifyEmptyStackTrace: Boolean = true,
  ): Unit = {
    val actualStatus = StatusProto.fromThrowable(actual)
    val actualDetails = actualStatus.getDetailsList.asScala.toSeq
    val cp = new Checkpoint
    cp { Statement.discard { actual.getStatus.getCode shouldBe expectedStatusCode } }
    cp { Statement.discard { actualStatus.getMessage shouldBe expectedMessage } }
    cp {
      Statement.discard {
        ErrorDetails.from(actualDetails) should contain theSameElementsAs expectedDetails
      }
    }
    if (verifyEmptyStackTrace) {
      cp {
        Statement.discard {
          actual.getStackTrace.length shouldBe 0 withClue ("it should contain no stacktrace")
        }
      }
    }
    cp { Statement.discard { actual.getCause shouldBe null } }
    cp.reportAll()
  }

}

trait ErrorAssertionsWithLogCollectorAssertions
    extends ErrorsAssertions
    with LogCollectorAssertions {
  self: Matchers =>

  def assertError[Test, Logger](
      actual: StatusRuntimeException,
      expectedCode: Code,
      expectedMessage: String,
      expectedDetails: Seq[ErrorDetails.ErrorDetail],
      expectedLogEntry: ExpectedLogEntry,
  )(implicit
      test: ClassTag[Test],
      logger: ClassTag[Logger],
  ): Unit = {
    assertError(
      actual = actual,
      expectedStatusCode = expectedCode,
      expectedMessage = expectedMessage,
      expectedDetails = expectedDetails,
    )
    val actualLogs: Seq[LogCollector.Entry] = LogCollector.readAsEntries(test, logger)
    actualLogs should have size 1
    assertLogEntry(actualLogs.head, expectedLogEntry)
  }

}
