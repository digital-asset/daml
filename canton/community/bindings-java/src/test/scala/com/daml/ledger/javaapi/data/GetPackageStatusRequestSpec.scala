// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v2.PackageServiceOuterClass.GetPackageStatusRequest as ProtoGetPackageStatusRequest
import org.scalacheck.Arbitrary
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetPackageStatusRequestSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "GetPackageStatusRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    Arbitrary.arbString.arbitrary
      .map(ProtoGetPackageStatusRequest.newBuilder().setPackageId(_).build())
  ) { packageRequest =>
    val converted =
      GetPackageStatusRequest.fromProto(packageRequest)
    GetPackageStatusRequest.fromProto(converted.toProto) shouldEqual converted
  }
}
