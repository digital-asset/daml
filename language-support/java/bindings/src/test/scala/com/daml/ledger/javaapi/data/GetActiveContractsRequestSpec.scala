// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class GetActiveContractsRequestSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {

  "GetActiveContractsRequestSpec.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getActiveContractRequestGen) { activeContractRequest =>
    val converted =
      GetActiveContractsRequest.fromProto(activeContractRequest)
    GetActiveContractsRequest.fromProto(converted.toProto) shouldEqual converted
  }
}
