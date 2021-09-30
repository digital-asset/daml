// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlConfiguration.DamlConfigurationSubmission
import com.daml.ledger.participant.state.kvutils.TestHelpers
import com.daml.ledger.participant.state.kvutils.TestHelpers._
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
    "produce rejection log entry if maximum record time is before record time" in {
      val instance = createConfigCommitter(aRecordTime)
      val context = createCommitContext(recordTime = Some(aRecordTime.addMicros(1)))

      val actual = instance.checkTtl(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail()
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasConfigurationRejectionEntry shouldBe true
          actualLogEntry.getConfigurationRejectionEntry.hasTimedOut shouldBe true
          actualLogEntry.getConfigurationRejectionEntry.getTimedOut.getMaximumRecordTime shouldBe buildTimestamp(
            aRecordTime
          )
      }
    }

    "continue if maximum record time is on or after record time" in {
      for (maximumRecordTime <- Iterable(aRecordTime, aRecordTime.addMicros(1))) {
        val instance = createConfigCommitter(maximumRecordTime)
        val context = createCommitContext(recordTime = Some(aRecordTime))

        val actual = instance.checkTtl(context, anEmptyResult)

        actual match {
          case StepContinue(_) => succeed
          case StepStop(_) => fail()
        }
      }
    }

    "skip checking against maximum record time if record time is not available" in {
      val instance = createConfigCommitter(aRecordTime)
      val context = createCommitContext(recordTime = None)

      val actual = instance.checkTtl(context, anEmptyResult)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail()
      }
    }

    "set the maximum record time and out-of-time-bounds log entry in the context if record time is not available" in {
      val instance = createConfigCommitter(aRecordTime)
      val context = createCommitContext(recordTime = None)

      instance.checkTtl(context, anEmptyResult)

      context.maximumRecordTime shouldEqual Some(aRecordTime.toInstant)
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

    "not set an out-of-time-bounds rejection log entry in case pre-execution is disabled" in {
      val instance = createConfigCommitter(theRecordTime)
      val context = createCommitContext(recordTime = Some(aRecordTime))

      instance.checkTtl(context, anEmptyResult)

      context.preExecute shouldBe false
      context.outOfTimeBoundsLogEntry shouldBe empty
    }
  }

  "buildLogEntry" should {
    "set record time in log entry when it is available" in {
      val instance = createConfigCommitter(theRecordTime.addMicros(1000))
      val context = createCommitContext(recordTime = Some(theRecordTime))

      val actual = instance.buildLogEntry(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail()
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasRecordTime shouldBe true
          actualLogEntry.getRecordTime shouldBe buildTimestamp(theRecordTime)
      }
    }

    "skip setting record time in log entry when it is not available" in {
      val instance = createConfigCommitter(theRecordTime)
      val context = createCommitContext(recordTime = None)

      val actual = instance.buildLogEntry(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail()
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasRecordTime shouldBe false
      }
    }

    "produce a log entry (pre-execution disabled or enabled)" in {
      for (recordTimeMaybe <- Iterable(Some(aRecordTime), None)) {
        val instance = createConfigCommitter(theRecordTime)
        val context = createCommitContext(recordTime = recordTimeMaybe)

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
  }

  private def createConfigCommitter(recordTime: Timestamp): ConfigCommitter =
    new ConfigCommitter(theDefaultConfig, recordTime, metrics)
}
