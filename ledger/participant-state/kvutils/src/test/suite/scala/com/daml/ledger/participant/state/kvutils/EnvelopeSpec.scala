// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.security.MessageDigest

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.participant.state.kvutils.{DamlKvutils => Proto}
import com.daml.lf.data.Ref.PackageId
import com.google.protobuf.ByteString
import org.scalatest.{Matchers, WordSpec}

class EnvelopeSpec extends WordSpec with Matchers {
  "envelope" should {

    "be able to enclose and open submissions" in {
      val submission = Proto.DamlSubmission.getDefaultInstance
      val compressed = Envelope.enclose(submission, compression = false)
      Envelope.open(compressed) shouldEqual Right(Envelope.SubmissionMessage(submission))
    }

    "be able to enclose and open log entries" in {
      val logEntry = Proto.DamlLogEntry.getDefaultInstance
      val compressed = Envelope.enclose(logEntry, compression = false)
      Envelope.open(compressed) shouldEqual Right(Envelope.LogEntryMessage(logEntry))
    }

    "be able to enclose and open state values" in {
      val stateValue = Proto.DamlStateValue.getDefaultInstance
      val compressed = Envelope.enclose(stateValue, compression = false)
      Envelope.open(compressed) shouldEqual Right(Envelope.StateValueMessage(stateValue))
    }

    "be able to enclose and open submission batch messages" in {
      val submissionBatch = Proto.DamlSubmissionBatch.newBuilder
        .addSubmissions(
          Proto.DamlSubmissionBatch.CorrelatedSubmission.newBuilder
            .setCorrelationId("anId")
            .setSubmission(ByteString.copyFromUtf8("a submission")))
        .build
      val compressed = Envelope.enclose(submissionBatch)
      Envelope.open(compressed) shouldEqual Right(Envelope.SubmissionBatchMessage(submissionBatch))
    }

    "compress and decompress" in {
      val oneMegabyte = 1024 * 1024
      val uncompressedSize = 100 * oneMegabyte
      val maximumCompressedSize = oneMegabyte
      val stateValue = {
        val payload = ByteString.copyFrom(Array.fill[Byte](uncompressedSize)(0))
        val hash = PackageId.assertFromString(
          MessageDigest
            .getInstance("SHA-256")
            .digest(payload.toByteArray)
            .map("%02x".format(_))
            .mkString)
        Proto.DamlStateValue
          .newBuilder()
          .setArchive(
            DamlLf.Archive
              .newBuilder()
              .setHashFunction(DamlLf.HashFunction.SHA256)
              .setPayload(payload)
              .setHash(hash))
          .build
      }

      val envelope = Envelope.enclose(stateValue, compression = true)
      envelope.size should be < maximumCompressedSize
      val opened = Envelope.open(envelope)
      opened shouldEqual Right(Envelope.StateValueMessage(stateValue))
    }
  }
}
