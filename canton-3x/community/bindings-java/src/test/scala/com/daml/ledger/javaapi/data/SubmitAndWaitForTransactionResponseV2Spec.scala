// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class SubmitAndWaitForTransactionResponseV2Spec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "SubmitAndWaitForTransactionResponse.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    submitAndWaitForTransactionResponseGen
  ) { response =>
    val converted =
      SubmitAndWaitForTransactionResponseV2.fromProto(response)
    SubmitAndWaitForTransactionResponseV2.fromProto(converted.toProto) shouldEqual converted
  }
}
