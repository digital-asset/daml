// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.lf.data.Ref
import com.google.protobuf.ByteString
import org.mockito.{Mockito, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class InMemorySubmissionAggregatorSpec extends AnyWordSpec with Matchers with MockitoSugar {
  "InMemorySubmissionAggregator" should {
    "aggregate data" in {
      val submissionInfo = SubmissionInfo(
        Ref.ParticipantId.assertFromString("participant-id"),
        "correlation ID",
        Raw.Envelope(ByteString.copyFromUtf8("the envelope")),
        Instant.now(),
      )
      val writer = mock[LedgerDataWriter]
      val submission = new InMemorySubmissionAggregator(submissionInfo, writer)
      val writeSetA = submission.addChild()
      writeSetA += keyValuePairOf("a", "b")
      writeSetA += keyValuePairOf("e", "f")

      val writeSetB = submission.addChild()
      writeSetB += keyValuePairOf("g", "h")
      writeSetB += keyValuePairOf("c", "d")

      submission.finish()

      val expected = Seq(
        keyValuePairOf("a", "b"),
        keyValuePairOf("e", "f"),
        keyValuePairOf("g", "h"),
        keyValuePairOf("c", "d"),
      )
      Mockito.verify(writer).write(submissionInfo, expected)
    }
  }

  private def keyValuePairOf(key: String, value: String): Raw.StateEntry =
    Raw.StateKey(ByteString.copyFromUtf8(key)) -> Raw.Envelope(ByteString.copyFromUtf8(value))
}
