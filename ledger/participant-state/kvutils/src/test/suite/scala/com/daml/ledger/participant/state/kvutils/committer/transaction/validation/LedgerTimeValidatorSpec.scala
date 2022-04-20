// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Err
import com.daml.ledger.participant.state.kvutils.TestHelpers.{
  createCommitContext,
  createEmptyTransactionEntry,
}
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Rejections,
}
import com.daml.ledger.participant.state.kvutils.store.events.DamlTransactionEntry
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LedgerTimeValidatorSpec extends AnyWordSpec with Matchers {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val metrics = new Metrics(new MetricRegistry)
  private val rejections = new Rejections(metrics)
  private val ledgerTimeValidationStep =
    new LedgerTimeValidator().createValidationStep(rejections)
  private val aDamlTransactionEntry: DamlTransactionEntry = createEmptyTransactionEntry(
    List("aSubmitter")
  )
  private val aTransactionEntrySummary = DamlTransactionEntrySummary(aDamlTransactionEntry)

  "LedgerTimeValidator" can {
    "when the record time is not available" should {
      "compute and correctly set out-of-time-bounds log entry with min/max record time available in the committer context" in {
        val context = createCommitContext()
        context.minimumRecordTime = Some(Timestamp.now())
        context.maximumRecordTime = Some(Timestamp.now())
        ledgerTimeValidationStep.apply(
          context,
          aTransactionEntrySummary,
        )

        context.outOfTimeBoundsLogEntry should not be empty
        context.outOfTimeBoundsLogEntry.foreach { actualOutOfTimeBoundsLogEntry =>
          actualOutOfTimeBoundsLogEntry.hasTransactionRejectionEntry shouldBe true
          actualOutOfTimeBoundsLogEntry.getTransactionRejectionEntry.hasRecordTimeOutOfRange shouldBe true
        }
      }

      "fail if minimum record time is not set" in {
        val context = createCommitContext()
        context.maximumRecordTime = Some(Timestamp.now())
        an[Err.InternalError] shouldBe thrownBy(
          ledgerTimeValidationStep.apply(
            context,
            aTransactionEntrySummary,
          )
        )
      }

      "fail if maximum record time is not set" in {
        val context = createCommitContext()
        context.minimumRecordTime = Some(Timestamp.now())
        an[Err.InternalError] shouldBe thrownBy(
          ledgerTimeValidationStep.apply(
            context,
            aTransactionEntrySummary,
          )
        )
      }
    }
  }
}
