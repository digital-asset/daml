// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GetUpdatesRequestSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "GetUpdatesRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    getUpdatesRequestGen
  ) { updatesRequest =>
    val converted =
      GetUpdatesRequest.fromProto(updatesRequest)
    GetUpdatesRequest.fromProto(converted.toProto) shouldEqual converted
  }
}
