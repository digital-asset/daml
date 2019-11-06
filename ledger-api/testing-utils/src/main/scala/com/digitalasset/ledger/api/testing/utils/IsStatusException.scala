// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import io.grpc.Status
import org.scalatest.{Assertion, Matchers}

import scala.util.control.NonFatal

object IsStatusException extends Matchers {

  def apply(expectedStatusCode: Status.Code)(throwable: Throwable): Assertion = {
    throwable match {
      case GrpcException(GrpcStatus(code, _), _) => code shouldEqual expectedStatusCode
      case NonFatal(other) => fail(s"$other is not a gRPC Status exception.")
    }
  }

  def apply(expectedStatus: Status): Throwable => Assertion = {
    apply(expectedStatus.getCode)
  }
}
