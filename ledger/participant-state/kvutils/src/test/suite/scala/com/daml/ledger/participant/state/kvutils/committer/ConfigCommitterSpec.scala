// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlConfigurationSubmission
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics
import org.scalatest.{Matchers, WordSpec}

class ConfigCommitterSpec extends WordSpec with Matchers {
  private val metrics = new Metrics(new MetricRegistry)
  private val aRecordTime = Timestamp(100)
  private val aConfigurationSubmission = DamlConfigurationSubmission.getDefaultInstance
  private val anEmptyResult =
    ConfigCommitter.Result(aConfigurationSubmission, (None, theDefaultConfig))

  "checkTtl" should {
    "produce rejection log entry if maximum record time is before record time" in {
      val instance = createConfigCommitter(aRecordTime)
      val context = new FakeCommitContext(recordTime = Some(aRecordTime.addMicros(1)))

      val actual = instance.checkTtl(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasConfigurationRejectionEntry shouldBe true
          actualLogEntry.getConfigurationRejectionEntry.hasTimedOut shouldBe true
          actualLogEntry.getConfigurationRejectionEntry.getTimedOut.getMaximumRecordTime shouldBe buildTimestamp(
            aRecordTime)
      }
    }

    "continue if maximum record time is on or after record time" in {
      for (maximumRecordTime <- Iterable(aRecordTime, aRecordTime.addMicros(1))) {
        val instance = createConfigCommitter(maximumRecordTime)
        val context = new FakeCommitContext(recordTime = Some(aRecordTime))

        val actual = instance.checkTtl(context, anEmptyResult)

        actual match {
          case StepContinue(_) => succeed
          case StepStop(_) => fail
        }
      }
    }

    val instance = createConfigCommitter(aRecordTime)
    val context = new FakeCommitContext(recordTime = None)

    "skip checking against maximum record time if record time is not available" in {
      instance.checkTtl(context, anEmptyResult) match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail
      }
    }

    "set the maximum record time in the context if record time is not available" in {
      instance.checkTtl(context, anEmptyResult)
      context.maximumRecordTime shouldEqual Some(aRecordTime.toInstant)
    }
  }

  "buildLogEntry" should {
    "set record time in log entry when it is available" in {
      val instance = createConfigCommitter(theRecordTime.addMicros(1000))
      val context = new FakeCommitContext(recordTime = Some(theRecordTime))

      val actual = instance.buildLogEntry(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasRecordTime shouldBe true
          actualLogEntry.getRecordTime shouldBe buildTimestamp(theRecordTime)
          actualLogEntry.hasConfigurationEntry shouldBe true
      }
    }

    "skip setting record time in log entry when it is not available" in {
      val instance = createConfigCommitter(theRecordTime)
      val context = new FakeCommitContext(recordTime = None)

      val actual = instance.buildLogEntry(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasRecordTime shouldBe false
          actualLogEntry.hasConfigurationEntry shouldBe true
      }
    }
  }

  private def createConfigCommitter(recordTime: Timestamp): ConfigCommitter =
    new ConfigCommitter(theDefaultConfig, recordTime, metrics)
}
