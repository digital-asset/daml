// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.*
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.error.TestGroup.NestedGroup.MyCode.MyError
import com.digitalasset.canton.error.TestGroup.NestedGroup.{MyCode, TestAlarmErrorCode}
import com.digitalasset.canton.logging.{ErrorLoggingContext, LogEntry}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status.Code
import org.slf4j.event.Level

object TestGroup extends ErrorGroup()(ErrorClass.root()) {
  object NestedGroup extends ErrorGroup() {
    object MyCode extends ErrorCode(id = "NESTED_CODE", ErrorCategory.ContentionOnSharedResources) {
      override def logLevel: Level = Level.ERROR
      final case class MyError(arg: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "this is my error")
    }

    object TestAlarmErrorCode extends AlarmErrorCode(id = "TEST_MALICIOUS_BEHAVIOR") {
      val exception = new RuntimeException("TestAlarmErrorCode exception")
      final case class MyAlarm()
          extends Alarm(cause = "My alarm cause", throwableO = Some(exception)) {}
    }
  }
}

class CantonErrorTest extends BaseTestWordSpec {

  "canton errors" should {
    "log proper error messages and provides a nice string" in {
      loggerFactory.assertLogs(
        MyError("testArg"),
        x => {
          x.errorMessage should include(MyCode.id)
          x.errorMessage should include("this is my error")
          x.mdc should contain(("arg" -> "testArg"))
          val loc = x.mdc.get("location")
          loc should not be empty
          loc.exists(_.startsWith("CantonErrorTest")) shouldBe true
        },
      )

    }

    "ship the context as part of the status and support decoding from exceptions" in {
      val err = loggerFactory.suppressErrors(MyError("testArg"))

      val status = DecodedRpcStatus.fromStatusRuntimeException(err.asGrpcError).value

      status.retryIn should not be empty
      status.context("arg") shouldBe "testArg"
      status.id shouldBe MyCode.id
      status.category shouldBe MyCode.category

    }

  }

  "canton error codes" should {
    "allow to recover the recoverability from the string" in {
      implicit val klass = new ErrorClass(Nil)
      val code = new ErrorCode(id = "TEST_ERROR", ErrorCategory.ContentionOnSharedResources) {}
      ErrorCodeUtils.errorCategoryFromString(code.toMsg("bla bla", None)) should contain(
        ErrorCategory.ContentionOnSharedResources
      )
    }

    "Extract code from multi-line strings" in {
      ErrorCodeUtils.errorCategoryFromString(
        "DB_CONNECTION_LOST(13,0):  It was not possible to establish a valid connection\nFull Error\n"
      ) should contain(
        ErrorCategory.BackgroundProcessDegradationWarning
      )
    }

  }

  "An alarm" should {
    "log with level WARN including the error category and details" in {
      implicit val traceContext: TraceContext = nonEmptyTraceContext1
      val traceId = traceContext.traceId.value
      val errorCodeStr = s"TEST_MALICIOUS_BEHAVIOR(5,${traceId.take(8)})"

      loggerFactory.assertLogs(
        TestAlarmErrorCode.MyAlarm().report(),
        entry => {
          entry.warningMessage shouldBe s"$errorCodeStr: My alarm cause"

          withClue(entry.mdc) {
            entry.mdc.get("location").value should startWith("CantonErrorTest.scala:")
            entry.mdc
              .get("err-context")
              .value should fullyMatch regex "\\{location=CantonErrorTest\\.scala:.*\\}"
            entry.mdc.get("error-code").value shouldBe errorCodeStr
            entry.mdc.get("trace-id").value shouldBe traceId
            entry.mdc.get("test").value shouldBe "CantonErrorTest"
          }
          entry.mdc should have size 5

          entry.throwable shouldBe Some(TestAlarmErrorCode.exception)
        },
      )
    }

    "not expose any details through GRPC" in {
      val myAlarm = TestAlarmErrorCode.MyAlarm()

      val sre = myAlarm.asGrpcError
      sre.getMessage shouldBe s"INVALID_ARGUMENT: ${LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API} <no-correlation-id> with tid <no-tid>"

      val status = sre.getStatus
      status.getDescription shouldBe s"${LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API} <no-correlation-id> with tid <no-tid>"
      status.getCode shouldBe Code.INVALID_ARGUMENT
      Option(status.getCause) shouldBe None
    }
  }
}
