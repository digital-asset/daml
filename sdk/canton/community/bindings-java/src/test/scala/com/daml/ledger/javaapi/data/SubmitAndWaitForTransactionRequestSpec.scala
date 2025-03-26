// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class SubmitAndWaitForTransactionRequestSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "SubmitAndWaitForTransactionRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    commandsGen,
    transactionFormatGen,
  ) { (commandsOuter, transactionFormatOuter) =>
    val commands = CommandsSubmission.fromProto(commandsOuter)
    val transactionFormat = TransactionFormat.fromProto(transactionFormatOuter)
    val request = new SubmitAndWaitForTransactionRequest(commands, transactionFormat)
    SubmitAndWaitForTransactionRequest.fromProto(request.toProto) shouldEqual request
  }
}
