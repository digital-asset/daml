// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.grpc.GrpcStatus
import com.google.rpc.error_details
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.scalatest._
import org.scalatest.matchers.should.Matchers

trait ValidatorTestUtils extends Matchers with Inside with OptionValues { self: Suite =>

  protected def requestMustFailWith(
      request: Either[StatusRuntimeException, _],
      code: Code,
      description: String,
      metadata: Map[String, String],
  ): Assertion = {
    inside(request)(isError(code, description, metadata))
  }
  protected def isError(
      expectedCode: Code,
      expectedDescription: String,
      metadata: Map[String, String],
  ): PartialFunction[Either[StatusRuntimeException, _], Assertion] = { case Left(err) =>
    err.getStatus should have(Symbol("code")(expectedCode))
    err.getStatus should have(Symbol("description")(expectedDescription))
    GrpcStatus
      .toProto(err.getStatus, err.getTrailers)
      .details
      .flatMap(_.unpack[error_details.ErrorInfo].metadata)
      .toMap should contain allElementsOf metadata
  }

}
