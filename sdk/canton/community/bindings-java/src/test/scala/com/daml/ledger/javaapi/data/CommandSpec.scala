// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class CommandSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "Command.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    commandGen
  ) { cmd =>
    val converted = Command.fromProtoCommand(cmd)
    Command.fromProtoCommand(converted.toProtoCommand) shouldEqual converted
  }
}
