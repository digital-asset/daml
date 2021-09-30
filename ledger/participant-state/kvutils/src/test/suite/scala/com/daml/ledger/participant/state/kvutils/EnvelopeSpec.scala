// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.wire._
import com.daml.ledger.participant.state.kvutils.{DamlKvutils => Proto, DamlState => ProtoState}
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EnvelopeSpec extends AnyWordSpec with Matchers {
  "envelope" should {

    "be able to enclose and open" in {
      val submission = DamlSubmission.getDefaultInstance

      Envelope.open(Envelope.enclose(submission)) shouldEqual
        Right(Envelope.SubmissionMessage(submission))

      val logEntry = Proto.DamlLogEntry.getDefaultInstance
      Envelope.open(Envelope.enclose(logEntry)) shouldEqual
        Right(Envelope.LogEntryMessage(logEntry))

      val stateValue = ProtoState.DamlStateValue.getDefaultInstance
      Envelope.open(Envelope.enclose(stateValue)) shouldEqual
        Right(Envelope.StateValueMessage(stateValue))
    }

    "be able to enclose and open batch submission batch message" in {
      val submissionBatch = DamlSubmissionBatch.newBuilder
        .addSubmissions(
          DamlSubmissionBatch.CorrelatedSubmission.newBuilder
            .setCorrelationId("anId")
            .setSubmission(ByteString.copyFromUtf8("a submission"))
        )
        .build
      Envelope.open(Envelope.enclose(submissionBatch)) shouldEqual
        Right(Envelope.SubmissionBatchMessage(submissionBatch))
    }
  }
}
