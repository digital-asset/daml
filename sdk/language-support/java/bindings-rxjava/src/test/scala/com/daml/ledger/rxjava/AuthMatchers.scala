// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava

import io.grpc.{Status, StatusException, StatusRuntimeException}
import org.scalactic.Equality
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.annotation.tailrec
import scala.util.control.NonFatal

private[rxjava] object AuthMatchers {

  private implicit object GrpcExceptionEquality extends Equality[Exception] {
    @tailrec
    private def equal(throwable: Throwable, code: Status.Code): Boolean =
      throwable match {
        case e: StatusException => e.getStatus.getCode == code
        case e: StatusRuntimeException => e.getStatus.getCode == code
        case NonFatal(e) => e.getCause != null && equal(e.getCause, code)
      }
    override def areEqual(exception: Exception, anything: Any): Boolean =
      anything match {
        case code: Status.Code => equal(exception, code)
        case _ => false
      }
  }

}

private[rxjava] trait AuthMatchers { self: Matchers =>

  import AuthMatchers.GrpcExceptionEquality

  private def expectError(code: Status.Code)(call: => Any): Assertion =
    the[Exception] thrownBy call shouldEqual code

  def expectUnauthenticated(call: => Any): Assertion =
    expectError(Status.Code.UNAUTHENTICATED)(call)

  def expectPermissionDenied(call: => Any): Assertion =
    expectError(Status.Code.PERMISSION_DENIED)(call)

  def expectDeadlineExceeded(call: => Any): Assertion =
    expectError(Status.Code.DEADLINE_EXCEEDED)(call)
}
