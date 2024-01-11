// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators._
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class TransactionFilterSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "TransactionFilter.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    transactionFilterGen
  ) { transactionFilter =>
    val converted = TransactionFilter.fromProto(transactionFilter)
    TransactionFilter.fromProto(converted.toProto) shouldEqual converted
  }
}
