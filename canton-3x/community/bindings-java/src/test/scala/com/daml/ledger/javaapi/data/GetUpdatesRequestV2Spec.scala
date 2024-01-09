// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetUpdatesRequestV2Spec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetUpdatesRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getUpdatesRequestGen
  ) { updatesRequest =>
    val converted =
      GetUpdatesRequestV2.fromProto(updatesRequest)
    GetUpdatesRequestV2.fromProto(converted.toProto) shouldEqual converted
  }
}
