// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.utils.ErrorDetails
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.testing.{LogCollector, LogCollectorAssertions}
import com.daml.platform.testing.LogCollector.ExpectedLogEntry
import com.daml.scalautil.Statement
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import org.scalatest.Checkpoints.Checkpoint

trait ErrorsAssertions {
  self: Matchers =>

  private val logger = ContextualizedLogger.get(getClass)
  private val loggingContext = LoggingContext.ForTesting
  private val errorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)

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
      expectedCode = expected.getStatus.getCode,
      expectedMessage = expectedStatus.getMessage,
      expectedDetails = ErrorDetails.from(expectedDetails),
    )
  }

  def assertError(
      actual: StatusRuntimeException,
      expectedCode: Code,
      expectedMessage: String,
      expectedDetails: Seq[ErrorDetails.ErrorDetail],
  ): Unit = {
    val actualStatus = StatusProto.fromThrowable(actual)
    val actualDetails = actualStatus.getDetailsList.asScala.toSeq
    val cp = new Checkpoint
    cp { Statement.discard { actual.getStatus.getCode shouldBe expectedCode } }
    cp { Statement.discard { actualStatus.getMessage shouldBe expectedMessage } }
    cp {
      Statement.discard {
        ErrorDetails.from(actualDetails) should contain theSameElementsAs expectedDetails
      }
    }
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
      expectedCode = expectedCode,
      expectedMessage = expectedMessage,
      expectedDetails = expectedDetails,
    )
    val actualLogs: Seq[LogCollector.Entry] = LogCollector.readAsEntries(test, logger)
    actualLogs should have size 1
    assertLogEntry(actualLogs.head, expectedLogEntry)
  }

}
