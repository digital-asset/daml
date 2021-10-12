// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.TestHelpers.{createCommitContext, theRecordTime}
import com.daml.ledger.participant.state.kvutils.store.events.DamlPartyAllocationEntry
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PartyAllocationCommitterSpec extends AnyWordSpec with Matchers {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val metrics = new Metrics(new MetricRegistry)
  private val aPartyAllocationEntry = DamlPartyAllocationEntry.newBuilder
    .setSubmissionId("an ID")
    .setParticipantId("a participant")

  "buildLogEntry" should {
    "produce an out-of-time-bounds rejection log entry in case pre-execution is enabled" in {
      val instance = new PartyAllocationCommitter(metrics)
      val context = createCommitContext(recordTime = None)

      instance.buildLogEntry(context, aPartyAllocationEntry)

      context.preExecute shouldBe true
      context.outOfTimeBoundsLogEntry should not be empty
      context.outOfTimeBoundsLogEntry.foreach { actual =>
        actual.hasRecordTime shouldBe false
        actual.hasPartyAllocationRejectionEntry shouldBe true
        actual.getPartyAllocationRejectionEntry.getSubmissionId shouldBe aPartyAllocationEntry.getSubmissionId
        actual.getPartyAllocationRejectionEntry.getParticipantId shouldBe aPartyAllocationEntry.getParticipantId
      }
    }

    "not set an out-of-time-bounds rejection log entry in case pre-execution is disabled" in {
      val instance = new PartyAllocationCommitter(metrics)
      val context = createCommitContext(recordTime = Some(theRecordTime))

      instance.buildLogEntry(context, aPartyAllocationEntry)

      context.preExecute shouldBe false
      context.outOfTimeBoundsLogEntry shouldBe empty
    }
  }
}
