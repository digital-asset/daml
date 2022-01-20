// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.utils.ErrorDetails
import com.daml.platform.testing.{LogCollector, LogCollectorAssertions}
import com.daml.platform.testing.LogCollector.ExpectedLogEntry
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

trait ErrorsAssertions {
  self: Matchers =>

  def assertError(
      actual: StatusRuntimeException,
      expectedCode: Code,
      expectedMessage: String,
      expectedDetails: Seq[ErrorDetails.ErrorDetail],
  ): Unit = {
    val status = StatusProto.fromThrowable(actual)
    status.getCode shouldBe expectedCode.value()
    status.getMessage shouldBe expectedMessage
    val details = status.getDetailsList.asScala.toSeq
    val _ = ErrorDetails.from(details) should contain theSameElementsAs expectedDetails
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
