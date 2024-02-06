// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetLedgerEndResponseSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetLedgerEndResponse.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getLedgerEndResponseGen
  ) { ledgerEndResponse =>
    val converted =
      GetLedgerEndResponse.fromProto(ledgerEndResponse)
    GetLedgerEndResponse.fromProto(converted.toProto) shouldEqual converted
  }
}
