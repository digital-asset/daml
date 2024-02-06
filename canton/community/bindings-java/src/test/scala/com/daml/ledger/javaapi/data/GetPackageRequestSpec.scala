// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v2.PackageServiceOuterClass.GetPackageRequest
import org.scalacheck.Arbitrary
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetPackageRequestSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetPackageRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    Arbitrary.arbString.arbitrary
      .map(GetPackageRequest.newBuilder().setPackageId(_).build())
  ) { packageRequest =>
    val converted =
      GetPackageRequest.fromProto(packageRequest)
    GetPackageRequest.fromProto(converted.toProto) shouldEqual converted
  }
}
