// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetTransactionByEventIdRequestSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetTransactionByEventIdRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getTransactionByEventIdRequestGen
  ) { transactionByEventIdRequest =>
    val converted =
      GetTransactionByEventIdRequest.fromProto(transactionByEventIdRequest)
    GetTransactionByEventIdRequest.fromProto(converted.toProto) shouldEqual converted
  }
}
