// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.TestHelpers
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.wire.DamlConfigurationSubmission
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConfigCommitterSpec extends AnyWordSpec with Matchers {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val metrics = new Metrics(new MetricRegistry)
  private val aRecordTime = Timestamp(100)
  private val aConfigurationSubmission = DamlConfigurationSubmission.newBuilder
    .setSubmissionId("an ID")
    .setParticipantId("a participant")
    .setConfiguration(Configuration.encode(TestHelpers.theDefaultConfig))
    .build
  private val anEmptyResult =
    ConfigCommitter.Result(aConfigurationSubmission, (None, theDefaultConfig))

  "checkTtl" should {

    "skip checking against maximum record time if record time is not available" in {
      val instance = createConfigCommitter(aRecordTime)
      val context = createCommitContext()

      val actual = instance.checkTtl(context, anEmptyResult)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail()
      }
    }

    "set the maximum record time and out-of-time-bounds log entry in the context" in {
      val instance = createConfigCommitter(aRecordTime)
      val context = createCommitContext()

      instance.checkTtl(context, anEmptyResult)

      context.maximumRecordTime shouldEqual Some(aRecordTime)
      context.outOfTimeBoundsLogEntry should not be empty
      context.outOfTimeBoundsLogEntry.foreach { actual =>
        actual.hasRecordTime shouldBe false
        actual.hasConfigurationRejectionEntry shouldBe true
        val actualConfigurationRejectionEntry = actual.getConfigurationRejectionEntry
        actualConfigurationRejectionEntry.getSubmissionId shouldBe aConfigurationSubmission.getSubmissionId
        actualConfigurationRejectionEntry.getParticipantId shouldBe aConfigurationSubmission.getParticipantId
        actualConfigurationRejectionEntry.getConfiguration shouldBe aConfigurationSubmission.getConfiguration
      }
    }
  }

  "buildLogEntry" should {

    "produce a log entry" in {
      val instance = createConfigCommitter(theRecordTime)
      val context = createCommitContext()

      val actual = instance.buildLogEntry(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail()
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasConfigurationEntry shouldBe true
          val actualConfigurationEntry = actualLogEntry.getConfigurationEntry
          actualConfigurationEntry.getSubmissionId shouldBe aConfigurationSubmission.getSubmissionId
          actualConfigurationEntry.getParticipantId shouldBe aConfigurationSubmission.getParticipantId
          actualConfigurationEntry.getConfiguration shouldBe aConfigurationSubmission.getConfiguration
          context.outOfTimeBoundsLogEntry should not be defined
      }
    }
  }

  private def createConfigCommitter(recordTime: Timestamp): ConfigCommitter =
    new ConfigCommitter(theDefaultConfig, recordTime, metrics)
}
