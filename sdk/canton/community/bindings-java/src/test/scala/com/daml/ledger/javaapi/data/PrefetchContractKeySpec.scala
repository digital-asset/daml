// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators.prefetchContractKeyGen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class PrefetchContractKeySpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "PrefetchContractKey.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    prefetchContractKeyGen
  ) { prefetchContractKey =>
    val converted = PrefetchContractKey.fromProto(prefetchContractKey)
    PrefetchContractKey.fromProto(converted.toProto) shouldEqual converted
  }
}
