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

    "compresses and decompresses quickly" in {
      val payload = ByteString.copyFrom(Array.fill[Byte](100 * 1024 * 1024)(0))
      val hash = PackageId.assertFromString(
        MessageDigest
          .getInstance("SHA-256")
          .digest(payload.toByteArray)
          .map("%02x".format(_))
          .mkString)
      val stateValue = Proto.DamlStateValue
        .newBuilder()
        .setArchive(
          DamlLf.Archive
            .newBuilder()
            .setHashFunction(DamlLf.HashFunction.SHA256)
            .setPayload(payload)
            .setHash(hash))
        .build()

      val envelope = timeAndLog("enclose", Envelope.enclose(stateValue, compression = true))
      val opened = timeAndLog("open", Envelope.open(envelope))
      opened shouldEqual Right(Envelope.StateValueMessage(stateValue))
    }
  }

  def timeAndLog[T](name: String, f: => T): T = {
    val start = System.nanoTime()
    val value = f
    val end = System.nanoTime()
    println(s"$name: ${(end - start) / 1000000}ms")
    value
  }
}
