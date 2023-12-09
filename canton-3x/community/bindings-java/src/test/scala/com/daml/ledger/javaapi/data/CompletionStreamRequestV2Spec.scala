// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2.completionStreamRequestGen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class CompletionStreamRequestV2Spec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "CompletionStreamRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    completionStreamRequestGen
  ) { completionStreamRequest =>
    val converted =
      CompletionStreamRequestV2.fromProto(completionStreamRequest)
    CompletionStreamRequestV2.fromProto(converted.toProto) shouldEqual converted
  }
}
