// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.utils.ErrorDetails
import com.daml.platform.testing.LogCollector.ExpectedLogEntry
import com.daml.platform.testing.{LogCollector, LogCollectorAssertions}
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

trait ErrorsAssertions {
  self: Matchers with LogCollectorAssertions =>

  def assertError(
      actual: StatusRuntimeException,
      expectedCode: Code,
      expectedMessage: String,
      expectedDetails: Seq[ErrorDetails.ErrorDetail],
  ): Assertion = {
    doAssertError(actual, expectedCode, expectedMessage, expectedDetails, None)
  }

  def assertError[Test, Logger](
      actual: StatusRuntimeException,
      expectedCode: Code,
      expectedMessage: String,
      expectedDetails: Seq[ErrorDetails.ErrorDetail],
      expectedLogEntry: ExpectedLogEntry,
  )(implicit
      test: ClassTag[Test],
      logger: ClassTag[Logger],
  ): Assertion = {
    doAssertError(actual, expectedCode, expectedMessage, expectedDetails, Some(expectedLogEntry))(
      test,
      logger,
    )
  }

  private def doAssertError[Test, Logger](
      actual: StatusRuntimeException,
      expectedCode: Code,
      expectedMessage: String,
      expectedDetails: Seq[ErrorDetails.ErrorDetail],
      expectedLogEntry: Option[ExpectedLogEntry],
  )(implicit
      test: ClassTag[Test],
      logger: ClassTag[Logger],
  ): Assertion = {
    val status = StatusProto.fromThrowable(actual)
    status.getCode shouldBe expectedCode.value()
    status.getMessage shouldBe expectedMessage
    val details = status.getDetailsList.asScala.toSeq
    val _ = ErrorDetails.from(details) should contain theSameElementsAs expectedDetails
    if (expectedLogEntry.isDefined) {
      val actualLogs: Seq[LogCollector.Entry] = LogCollector.readAsEntries(test, logger)
      actualLogs should have size 1
      assertLogEntry(actualLogs.head, expectedLogEntry.get)
    }
    succeed
  }

}
