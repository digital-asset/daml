// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators._
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class GetActiveContractsRequestSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetActiveContractsRequestSpec.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getActiveContractRequestGen
  ) { activeContractRequest =>
    val converted =
      GetActiveContractsRequest.fromProto(activeContractRequest)
    GetActiveContractsRequest.fromProto(converted.toProto) shouldEqual converted
  }
}
