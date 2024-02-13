// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetTransactionTreeResponseSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetTransactionTreeResponse.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getTransactionTreeResponseGen
  ) { transactionTreeResponse =>
    val converted =
      GetTransactionTreeResponse.fromProto(transactionTreeResponse)
    GetTransactionTreeResponse.fromProto(converted.toProto) shouldEqual converted
  }
}
