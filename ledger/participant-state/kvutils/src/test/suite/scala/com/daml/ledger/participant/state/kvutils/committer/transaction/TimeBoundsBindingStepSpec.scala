// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions.configurationStateKey
import com.daml.ledger.participant.state.kvutils.TestHelpers.{
  createCommitContext,
  createEmptyTransactionEntry,
  theDefaultConfig,
}
import com.daml.ledger.participant.state.kvutils.committer.{StepContinue, StepStop}
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlConfigurationEntry,
  DamlTransactionEntry,
}
import com.daml.ledger.participant.state.kvutils.store.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Conversions, TestHelpers}
import com.daml.lf.data.Time
import com.daml.logging.LoggingContext
import com.google.protobuf.timestamp
import com.google.protobuf.timestamp.Timestamp.toJavaProto
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class TimeBoundsBindingStepSpec extends AnyWordSpec with Matchers {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val step = TimeBoundBindingStep.setTimeBoundsInContextStep(TestHelpers.theDefaultConfig)
  private val aDamlTransactionEntry = createEmptyTransactionEntry(List("aSubmitter"))
  private val aTransactionEntrySummary = DamlTransactionEntrySummary(aDamlTransactionEntry)
  private val aSubmissionTime = createProtobufTimestamp(seconds = 1)
  private val aLedgerEffectiveTime = createProtobufTimestamp(seconds = 2)
  private val aDamlTransactionEntryWithSubmissionAndLedgerEffectiveTimes: DamlTransactionEntry =
    aDamlTransactionEntry.toBuilder
      .setSubmissionTime(toJavaProto(aSubmissionTime))
      .setLedgerEffectiveTime(toJavaProto(aLedgerEffectiveTime))
      .build()
  private val aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes =
    DamlTransactionEntrySummary(aDamlTransactionEntryWithSubmissionAndLedgerEffectiveTimes)

  private val emptyConfigurationStateValue: DamlStateValue =
    defaultConfigurationStateValueBuilder().build

  private val inputWithConfiguration: Map[DamlStateKey, Some[DamlStateValue]] =
    Map(
      Conversions.configurationStateKey -> Some(emptyConfigurationStateValue)
    )
  private val inputWithTimeModelAndEmptyCommandDeduplication =
    Map(Conversions.configurationStateKey -> Some(emptyConfigurationStateValue))

  "time bounds binding step" should {
    "continue" in {
      val result = step(
        contextWithTimeModelAndEmptyCommandDeduplication(),
        aTransactionEntrySummary,
      )

      result match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail()
      }
    }

    "compute and correctly set the min/max ledger time" in {
      val context = contextWithTimeModelAndEmptyCommandDeduplication()

      step.apply(
        context,
        aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes,
      )

      context.minimumRecordTime shouldEqual Some(
        protoTimestampToLf(aLedgerEffectiveTime)
          .subtract(theDefaultConfig.timeModel.minSkew)
      )
      context.maximumRecordTime shouldEqual Some(
        protoTimestampToLf(aSubmissionTime)
          .add(theDefaultConfig.timeModel.maxSkew)
      )
    }

    "mark config as accessed in context" in {
      val commitContext =
        createCommitContext(recordTime = None, inputWithConfiguration)

      step.apply(commitContext, aTransactionEntrySummary)

      commitContext.getAccessedInputKeys should contain(configurationStateKey)
    }
  }

  private def contextWithTimeModelAndEmptyCommandDeduplication() =
    createCommitContext(recordTime = None, inputs = inputWithTimeModelAndEmptyCommandDeduplication)

  private def createProtobufTimestamp(seconds: Long): timestamp.Timestamp = {
    timestamp.Timestamp(seconds)
  }
  private def defaultConfigurationStateValueBuilder(): DamlStateValue.Builder =
    DamlStateValue.newBuilder
      .setConfigurationEntry(
        DamlConfigurationEntry.newBuilder
          .setConfiguration(Configuration.encode(theDefaultConfig))
      )

  private def protoTimestampToLf(ts: timestamp.Timestamp) =
    Time.Timestamp.assertFromLong(ts.seconds.seconds.toMicros)
}
