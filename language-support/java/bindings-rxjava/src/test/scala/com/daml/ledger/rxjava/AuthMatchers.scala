// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava

import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import org.scalatest.{Assertion, Matchers}

private[rxjava] trait AuthMatchers { self: Matchers =>

  private def theCausalChainOf(t: Throwable): Iterator[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)

  def expectPermissionDenied(blockingAuthenticatedCall: => Any): Assertion =
    theCausalChainOf(the[RuntimeException] thrownBy { blockingAuthenticatedCall }) collect {
      case GrpcException(GrpcStatus.PERMISSION_DENIED(), _) => ()
    } should not be empty

}
