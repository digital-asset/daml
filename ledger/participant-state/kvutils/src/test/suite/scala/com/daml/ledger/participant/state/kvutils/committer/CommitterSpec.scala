// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.google.protobuf.ByteString
import org.scalatest.{Matchers, WordSpec}

class CommitterSpec extends WordSpec with Matchers {
  private class TestCommitter(outputKeys: Seq[String]) extends Committer[Bytes, Bytes] {
    private val addKeysStep: Step = (context: CommitContext, _) => {
      val damlStateValue = DamlStateValue.getDefaultInstance
      for (outputKey <- outputKeys) {
        val outputDamlStateKey = DamlStateKey.newBuilder
          .setContractId(outputKey)
          .build
        context.set(outputDamlStateKey, damlStateValue)
      }
      StepStop(DamlLogEntry.getDefaultInstance)
    }

    override def steps: Iterable[(StepInfo, Step)] = Seq(("add keys", addKeysStep))

    override def init(ctx: CommitContext, submission: Bytes): Bytes = submission
  }

  "Committer" should {
    "return output states in sorted order" in {
      val instance = new TestCommitter(Seq("b", "c", "a"))
      val (_, actualStateUpdates) = instance.run(
        DamlLogEntryId.getDefaultInstance,
        Time.Timestamp.MaxValue,
        Time.Timestamp.MinValue,
        ByteString.copyFromUtf8("aSubmission"),
        Ref.ParticipantId.assertFromString("aParticipantId"),
        Map.empty
      )
      actualStateUpdates.map(_._1.getContractId) shouldBe Seq("a", "b", "c")
    }
  }
}
