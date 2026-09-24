// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators.*
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.jdk.OptionConverters.*

class SubmitAndWaitForReassignmentRequestSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "SubmitAndWaitForReassignmentRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    reassignmentCommandsGen,
    Gen.option(eventFormatGen),
  ) { (commandsOuter, eventFormatOuter) =>
    val commands = ReassignmentCommands.fromProto(commandsOuter)
    val eventFormat = eventFormatOuter.map(EventFormat.fromProto).toJava
    val request = new SubmitAndWaitForReassignmentRequest(commands, eventFormat)
    SubmitAndWaitForReassignmentRequest.fromProto(request.toProto) shouldEqual request
  }
}
