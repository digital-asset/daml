// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class TransactionFilterSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "TransactionFilter.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    transactionFilterGen) { transactionFilter =>
    val converted = TransactionFilter.fromProto(transactionFilter)
    TransactionFilter.fromProto(converted.toProto) shouldEqual converted
  }
}
