// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import io.grpc.{Status, StatusException, StatusRuntimeException}
import org.scalatest.{Assertion, Matchers}

object IsStatusException extends Matchers {

  def apply(expectedStatusCode: Status.Code)(throwable: Throwable): Assertion = {
    throwable match {
      case s: StatusRuntimeException => s.getStatus.getCode shouldEqual expectedStatusCode
      case s: StatusException => s.getStatus.getCode shouldEqual expectedStatusCode
      case other => fail(s"$other is not a gRPC Status exception.")
    }
  }

  def apply(expectedStatus: Status): Throwable => Assertion = {
    apply(expectedStatus.getCode)
  }
}
