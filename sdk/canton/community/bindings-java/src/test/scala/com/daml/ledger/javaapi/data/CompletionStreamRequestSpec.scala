// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators.completionStreamRequestGen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class CompletionStreamRequestSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "CompletionStreamRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    completionStreamRequestGen
  ) { completionStreamRequest =>
    val converted =
      CompletionStreamRequest.fromProto(completionStreamRequest)
    CompletionStreamRequest.fromProto(converted.toProto) shouldEqual converted
  }
}
