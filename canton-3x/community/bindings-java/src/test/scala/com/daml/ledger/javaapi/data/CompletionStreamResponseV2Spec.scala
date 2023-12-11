// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2.completionStreamResponseGen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class CompletionStreamResponseV2Spec
  extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "CompletionStreamRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    completionStreamResponseGen
  ) { completionStreamResponseGen =>
    val converted =
      CompletionStreamResponseV2.fromProto(completionStreamResponseGen)
    CompletionStreamResponseV2.fromProto(converted.toProto) shouldEqual converted
  }
}
