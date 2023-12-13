// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetTransactionTreeResponseV2Spec
  extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetTransactionTreeResponse.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getTransactionTreeResponseGen
  ) { transactionTreeResponse =>
    val converted =
      GetTransactionTreeResponseV2.fromProto(transactionTreeResponse)
    GetTransactionTreeResponseV2.fromProto(converted.toProto) shouldEqual converted
  }
}
