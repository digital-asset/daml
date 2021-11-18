// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import java.time

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions.{buildDuration, buildTimestamp}
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepStop}
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlConfigurationEntry,
  DamlSubmitterInfo,
}
import com.daml.ledger.participant.state.kvutils.store.{DamlCommandDedupValue, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf
import org.mockito.MockitoSugar
import org.scalatest.Inside.inside
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn
import scala.util.Random

@nowarn("msg=deprecated")
class CommandDeduplicationSpec
    extends AnyWordSpec
    with Matchers
    with MockitoSugar
    with OptionValues {
  import CommandDeduplicationSpec._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val metrics = new Metrics(new MetricRegistry)
  private val rejections = new Rejections(metrics)
  private val deduplicateCommandStep = CommandDeduplication.deduplicateCommandStep(rejections)
  private val setDeduplicationEntryStep =
    CommandDeduplication.setDeduplicationEntryStep(theDefaultConfig)

  "deduplicateCommand" should {
    "continue if record time is not available" in {
      val context = createCommitContext(recordTime = None)

      val actual =
        deduplicateCommandStep(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail()
      }
    }

    "continue if record time is available but no deduplication entry could be found" in {
      val inputs = Map(aDedupKey -> None)
      val context =
        createCommitContext(recordTime = Some(aRecordTime), inputs = inputs)

      val actual = deduplicateCommandStep(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail()
      }
    }

    "continue if record time is after deduplication time in case a deduplication entry is found" in {
      val dedupValue = newDedupValue(aRecordTime)
      val inputs = Map(aDedupKey -> Some(dedupValue))
      val context =
        createCommitContext(recordTime = Some(aRecordTime.addMicros(1)), inputs = inputs)

      val actual = deduplicateCommandStep(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail()
      }
    }

    "produce rejection log entry in case record time is on or before deduplication time" in {
      for (
        (recordTime, deduplicationTime) <- Iterable(
          (aRecordTime, aRecordTime),
          (aRecordTime, aRecordTime.addMicros(1)),
        )
      ) {
        val dedupValue = newDedupValue(deduplicationTime)
        val inputs = Map(aDedupKey -> Some(dedupValue))
        val context =
          createCommitContext(recordTime = Some(recordTime), inputs = inputs)

        val actual = deduplicateCommandStep(context, aTransactionEntrySummary)

        actual match {
          case StepContinue(_) => fail()
          case StepStop(actualLogEntry) =>
            actualLogEntry.hasTransactionRejectionEntry shouldBe true
        }
      }
    }

    "setting dedup context" should {
      val deduplicateUntil = protobuf.Timestamp.newBuilder().setSeconds(30).build()
      val submissionTime = protobuf.Timestamp.newBuilder().setSeconds(60).build()
      val deduplicationDuration = time.Duration.ofSeconds(3)

      "calculate deduplicate until based on deduplication duration" in {
        val (context, transactionEntrySummary) =
          buildContextAndTransaction(
            submissionTime,
            _.setDeduplicationDuration(Conversions.buildDuration(deduplicationDuration)),
          )
        setDeduplicationEntryStep(context, transactionEntrySummary)
        contextDeduplicateUntil(
          context,
          transactionEntrySummary,
        ).value shouldBe protobuf.Timestamp
          .newBuilder()
          .setSeconds(
            submissionTime.getSeconds + deduplicationDuration.getSeconds + theDefaultConfig.timeModel.minSkew.getSeconds
          )
          .build()
      }

      "set the submission time in the committer context" in {
        val (context, transactionEntrySummary) =
          buildContextAndTransaction(
            submissionTime,
            _.setDeduplicationDuration(Conversions.buildDuration(deduplicationDuration)),
          )
        setDeduplicationEntryStep(context, transactionEntrySummary)
        context
          .get(Conversions.commandDedupKey(transactionEntrySummary.submitterInfo))
          .map(
            _.getCommandDedup.getSubmissionTime
          )
          .value shouldBe submissionTime
      }

      "throw an error for unsupported deduplication periods" in {
        forAll(
          Table[DamlSubmitterInfo.Builder => DamlSubmitterInfo.Builder](
            "deduplication setter",
            _.clearDeduplicationPeriod(),
            _.setDeduplicationOffset("offset"),
            _.setDeduplicateUntil(deduplicateUntil),
          )
        ) { deduplicationSetter =>
          {
            val (context, transactionEntrySummary) =
              buildContextAndTransaction(submissionTime, deduplicationSetter)
            a[Err.InvalidSubmission] shouldBe thrownBy(
              setDeduplicationEntryStep(context, transactionEntrySummary)
            )
          }
        }
      }
    }

    "overwriteDeduplicationPeriodWithMaxDuration" should {
      "set max deduplication duration as deduplication period" in {
        val maxDeduplicationDuration = time.Duration.ofSeconds(Random.nextLong())
        val config = theDefaultConfig.copy(maxDeduplicationTime = maxDeduplicationDuration)
        val commitContext = createCommitContext(
          None,
          Map(
            Conversions.configurationStateKey -> None
          ),
        )
        val result =
          CommandDeduplication.overwriteDeduplicationPeriodWithMaxDurationStep(config)(
            commitContext,
            aTransactionEntrySummary,
          )
        inside(result) { case StepContinue(entry) =>
          entry.submitterInfo.getDeduplicationDuration shouldBe buildDuration(
            maxDeduplicationDuration
          )
        }
      }
    }
  }

  private def newDedupValue(deduplicationTime: Timestamp): DamlStateValue =
    DamlStateValue.newBuilder
      .setCommandDedup(
        DamlCommandDedupValue.newBuilder.setDeduplicatedUntil(buildTimestamp(deduplicationTime))
      )
      .build
}

object CommandDeduplicationSpec {

  private val aDamlTransactionEntry = createEmptyTransactionEntry(List("aSubmitter"))
  private val aTransactionEntrySummary = DamlTransactionEntrySummary(aDamlTransactionEntry)
  private val aRecordTime = Timestamp(100)
  private val aDedupKey = Conversions
    .commandDedupKey(aTransactionEntrySummary.submitterInfo)
  private val aDamlConfigurationStateValue = DamlStateValue.newBuilder
    .setConfigurationEntry(
      DamlConfigurationEntry.newBuilder
        .setConfiguration(Configuration.encode(theDefaultConfig))
    )
    .build

  private def buildContextAndTransaction(
      submissionTime: protobuf.Timestamp,
      submitterInfoAugmenter: DamlSubmitterInfo.Builder => DamlSubmitterInfo.Builder,
  ) = {
    val context = createCommitContext(None)
    context.set(Conversions.configurationStateKey, aDamlConfigurationStateValue)
    val transactionEntrySummary = DamlTransactionEntrySummary(
      aDamlTransactionEntry.toBuilder
        .setSubmitterInfo(
          submitterInfoAugmenter(
            DamlSubmitterInfo
              .newBuilder()
          )
        )
        .setSubmissionTime(submissionTime)
        .build()
    )
    context -> transactionEntrySummary
  }

  private def contextDeduplicateUntil(
      context: CommitContext,
      transactionEntrySummary: DamlTransactionEntrySummary,
  ) = context
    .get(Conversions.commandDedupKey(transactionEntrySummary.submitterInfo))
    .map(
      _.getCommandDedup.getDeduplicatedUntil
    )
}
