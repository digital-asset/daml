// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlPackageUploadEntry
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class PackageCommitterSpec extends WordSpec with Matchers with MockitoSugar {
  private val metrics = new Metrics(new MetricRegistry)
  private val anEmptyResult = DamlPackageUploadEntry.newBuilder
    .setSubmissionId("an ID")
    .setParticipantId("a participant")

  "buildLogEntry" should {
    "set record time in log entry if record time is available" in {
      val instance = new PackageCommitter(mock[Engine], metrics)
      val context = new FakeCommitContext(recordTime = Some(theRecordTime))

      val actual = instance.buildLogEntry(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasRecordTime shouldBe true
          actualLogEntry.getRecordTime shouldBe buildTimestamp(theRecordTime)
      }
    }

    "skip setting record time in log entry when it is not available" in {
      val instance = new PackageCommitter(mock[Engine], metrics)
      val context = new FakeCommitContext(recordTime = None)

      val actual = instance.buildLogEntry(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasRecordTime shouldBe false
      }
    }
    "produce an out-of-time-bounds rejection log entry in case pre-execution is enabled" in {
      val instance = new PackageCommitter(mock[Engine], metrics)
      val context = new FakeCommitContext(recordTime = None)

      instance.buildLogEntry(context, anEmptyResult)

      context.preExecute shouldBe true
      context.outOfTimeBoundsLogEntry should not be empty
      context.outOfTimeBoundsLogEntry.foreach { actual =>
        actual.hasRecordTime shouldBe false
        actual.hasPackageUploadRejectionEntry shouldBe true
        actual.getPackageUploadRejectionEntry.getSubmissionId shouldBe anEmptyResult.getSubmissionId
        actual.getPackageUploadRejectionEntry.getParticipantId shouldBe anEmptyResult.getParticipantId
      }
    }

    "not set an out-of-time-bounds rejection log entry in case pre-execution is disabled" in {
      val instance = new PackageCommitter(mock[Engine], metrics)
      val context = new FakeCommitContext(recordTime = Some(theRecordTime))

      instance.buildLogEntry(context, anEmptyResult)

      context.preExecute shouldBe false
      context.outOfTimeBoundsLogEntry shouldBe empty
    }
  }
}
