// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.{DamlKvutils => Proto}
import com.google.protobuf.ByteString
import org.scalatest.{Matchers, WordSpec}

class EnvelopeSpec extends WordSpec with Matchers {
  "envelope" should {

    "be able to enclose and open" in {
      val submission = Proto.DamlSubmission.getDefaultInstance

      Envelope.open(Envelope.enclose(submission)) shouldEqual
        Right(Envelope.SubmissionMessage(submission))

      val logEntry = Proto.DamlLogEntry.getDefaultInstance
      Envelope.open(Envelope.enclose(logEntry)) shouldEqual
        Right(Envelope.LogEntryMessage(logEntry))

      val stateValue = Proto.DamlStateValue.getDefaultInstance
      Envelope.open(Envelope.enclose(stateValue)) shouldEqual
        Right(Envelope.StateValueMessage(stateValue))
    }

    "be able to enclose and open batch submission batch message" in {
      val submissionBatch = Proto.DamlSubmissionBatch.newBuilder
        .addSubmissions(
          Proto.DamlSubmissionBatch.CorrelatedSubmission.newBuilder
            .setCorrelationId("anId")
            .setSubmission(ByteString.copyFromUtf8("a submission")))
        .build
      Envelope.open(Envelope.enclose(submissionBatch)) shouldEqual
        Right(Envelope.SubmissionBatchMessage(submissionBatch))
    }
  }
}
