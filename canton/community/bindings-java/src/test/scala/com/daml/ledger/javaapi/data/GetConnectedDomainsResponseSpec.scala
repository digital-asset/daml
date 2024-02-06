// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetConnectedDomainsResponseSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetConnectedDomainsResponse.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getConnectedDomainsResponseGen
  ) { connectedDomainsResponse =>
    val converted =
      GetConnectedDomainsResponse.fromProto(connectedDomainsResponse)
    GetConnectedDomainsResponse.fromProto(converted.toProto) shouldEqual converted
  }
}
