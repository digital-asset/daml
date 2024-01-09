// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass.SubmitReassignmentRequest
import com.daml.ledger.javaapi.data.GeneratorsV2.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class SubmitReassignmentRequestV2Spec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "SubmitReassignmentRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    reassignmentCommandGen.map(
      SubmitReassignmentRequest.newBuilder().setReassignmentCommand(_).build
    )
  ) { request =>
    val commands = SubmitReassignmentRequestV2.fromProto(request)
    SubmitReassignmentRequestV2.fromProto(
      SubmitReassignmentRequestV2.toProto(commands)
    ) shouldEqual commands
  }
}
