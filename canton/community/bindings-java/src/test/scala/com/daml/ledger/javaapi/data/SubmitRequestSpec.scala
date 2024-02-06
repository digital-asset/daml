// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass.SubmitRequest
import com.daml.ledger.javaapi.data.GeneratorsV2.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class SubmitRequestSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "SubmitRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    commandsGen.map(SubmitRequest.newBuilder().setCommands(_).build)
  ) { request =>
    val commands = SubmitRequest.fromProto(request)
    SubmitRequest.fromProto(SubmitRequest.toProto(commands)) shouldEqual commands
  }
}
