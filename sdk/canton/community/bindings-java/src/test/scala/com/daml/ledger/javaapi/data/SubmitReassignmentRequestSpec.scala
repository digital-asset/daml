// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass.SubmitReassignmentRequest as SubmitReassignmentRequestProto
import com.daml.ledger.javaapi.data.Generators.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class SubmitReassignmentRequestSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "SubmitReassignmentRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    reassignmentCommandGen.map(
      SubmitReassignmentRequestProto.newBuilder().setReassignmentCommand(_).build
    )
  ) { request =>
    val commands = SubmitReassignmentRequest.fromProto(request)
    SubmitReassignmentRequest.fromProto(
      SubmitReassignmentRequest.toProto(commands)
    ) shouldEqual commands
  }
}
