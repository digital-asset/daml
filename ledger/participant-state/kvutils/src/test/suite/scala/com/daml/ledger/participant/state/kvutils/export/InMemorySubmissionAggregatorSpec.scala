// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.google.protobuf.ByteString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class InMemorySubmissionAggregatorSpec extends WordSpec with Matchers with MockitoSugar {
  "InMemorySubmissionAggregatorSpec" should {
    "aggregate data" in {
      val submission = new InMemorySubmissionAggregator()
      val writeSetA = submission.addChild()
      writeSetA += keyValuePairOf("a", "b")
      writeSetA += keyValuePairOf("c", "d")

      val writeSetB = submission.addChild()
      writeSetB += keyValuePairOf("e", "f")
      writeSetB += keyValuePairOf("g", "h")

      submission.finish() should be(
        Seq(
          keyValuePairOf("a", "b"),
          keyValuePairOf("c", "d"),
          keyValuePairOf("e", "f"),
          keyValuePairOf("g", "h"),
        ))
    }
  }

  private def keyValuePairOf(key: String, value: String): (ByteString, ByteString) =
    ByteString.copyFromUtf8(key) -> ByteString.copyFromUtf8(value)
}
