// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.`Bytes Ordering`
import com.daml.ledger.participant.state.v1.ParticipantId
import com.google.protobuf.ByteString
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.SortedMap

final class InMemorySubmissionAggregatorSpec extends WordSpec with Matchers with MockitoSugar {
  "InMemorySubmissionAggregator" should {
    "aggregate data" in {
      val submissionInfo = SubmissionInfo(
        ParticipantId.assertFromString("participant-id"),
        "correlation ID",
        ByteString.copyFromUtf8("the envelope"),
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

      val expected = SortedMap(
        keyValuePairOf("a", "b"),
        keyValuePairOf("c", "d"),
        keyValuePairOf("e", "f"),
        keyValuePairOf("g", "h"),
      )
      Mockito.verify(writer).write(submissionInfo, expected)
    }
  }

  private def keyValuePairOf(key: String, value: String): (ByteString, ByteString) =
    ByteString.copyFromUtf8(key) -> ByteString.copyFromUtf8(value)
}
