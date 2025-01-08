// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators.completionStreamResponseGen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class CompletionStreamResponseSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "CompletionStreamRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    completionStreamResponseGen
  ) { completionStreamResponseGen =>
    val converted =
      CompletionStreamResponse.fromProto(completionStreamResponseGen)
    CompletionStreamResponse.fromProto(converted.toProto) shouldEqual converted
  }
}
