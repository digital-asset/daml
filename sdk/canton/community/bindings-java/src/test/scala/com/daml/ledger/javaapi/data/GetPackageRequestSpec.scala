// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v2.PackageServiceOuterClass.GetPackageRequest as ProtoGetPackageRequest
import org.scalacheck.Arbitrary
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetPackageRequestSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "GetPackageRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    Arbitrary.arbString.arbitrary
      .map(ProtoGetPackageRequest.newBuilder().setPackageId(_).build())
  ) { packageRequest =>
    val converted =
      GetPackageRequest.fromProto(packageRequest)
    GetPackageRequest.fromProto(converted.toProto) shouldEqual converted
  }
}
