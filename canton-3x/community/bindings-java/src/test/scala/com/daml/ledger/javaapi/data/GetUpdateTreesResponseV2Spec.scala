// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetUpdateTreesResponseV2Spec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetUpdateTreesResponse.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getUpdateTreesResponseGen
  ) { updateTreesResponse =>
    val converted =
      GetUpdateTreesResponseV2.fromProto(updateTreesResponse)
    GetUpdateTreesResponseV2.fromProto(converted.toProto) shouldEqual converted
  }
}
