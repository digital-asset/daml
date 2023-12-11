// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetEventsByContractIdResponseV2Spec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetEventsByContractIdResponse.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getEventsByContractIdResponseGen
  ) { eventsByContractIdResponse =>
    val converted =
      GetEventsByContractIdResponseV2.fromProto(eventsByContractIdResponse)
    GetEventsByContractIdResponseV2.fromProto(converted.toProto) shouldEqual converted
  }
}
