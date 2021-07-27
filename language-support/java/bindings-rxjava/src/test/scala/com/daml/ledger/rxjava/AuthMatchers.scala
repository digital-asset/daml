// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava

import com.daml.grpc.{GrpcException, GrpcStatus}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

private[rxjava] trait AuthMatchers { self: Matchers =>

  private def theCausalChainOf(t: Throwable): Iterator[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)

  private def expectError(predicate: Throwable => Boolean)(call: => Any): Assertion =
    theCausalChainOf(the[RuntimeException] thrownBy call).filter(predicate) should not be empty

  def expectUnauthenticated(call: => Any): Assertion =
    expectError {
      case GrpcException(GrpcStatus.UNAUTHENTICATED(), _) => true
      case _ => false
    }(call)

  def expectPermissionDenied(call: => Any): Assertion =
    expectError {
      case GrpcException(GrpcStatus.PERMISSION_DENIED(), _) => true
      case _ => false
    }(call)

  def expectDeadlineExceeded(call: => Any): Assertion =
    expectError {
      case GrpcException(GrpcStatus.DEADLINE_EXCEEDED(), _) => true
      case _ => false
    }(call)
}
