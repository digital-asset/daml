// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetTransactionByEventIdRequestV2Spec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetTransactionByEventIdRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getTransactionByEventIdRequestGen
  ) { transactionByEventIdRequest =>
    val converted =
      GetTransactionByEventIdRequestV2.fromProto(transactionByEventIdRequest)
    GetTransactionByEventIdRequestV2.fromProto(converted.toProto) shouldEqual converted
  }
}
