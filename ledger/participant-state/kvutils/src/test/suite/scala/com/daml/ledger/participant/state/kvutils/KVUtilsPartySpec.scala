// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.store.DamlLogEntry
import com.daml.ledger.participant.state.kvutils.store.events.DamlPartyAllocationRejectionEntry
import com.daml.logging.LoggingContext
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KVUtilsPartySpec extends AnyWordSpec with Matchers {
  // TESTS:
  // - party allocation rejected if participant id does not match
  // - party allocation rejected with bad party string
  // - party allocation succeeds

  import KVTest._
  import TestHelpers._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "party allocation" should {
    val p0 = mkParticipantId(0)
    val p1 = mkParticipantId(1)

    "be able to pre-execute a party allocation" in KVTest.runTest {
      withParticipantId(p0) {
        preExecutePartyAllocation("ok", "alice", p0).map { preExecutionResult =>
          preExecutionResult.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY
        }
      }
    }

    "reject when participant id does not match" in KVTest.runTest {
      withParticipantId(p0) {
        preExecutePartyAllocation("mismatch", "alice", p1)
      }.map { logEntry =>
        logEntry.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY
      }
    }

    "reject on bad party string" in KVTest.runTest {
      for {
        result1 <- preExecutePartyAllocation("empty party", "", p0)
        result2 <- preExecutePartyAllocation("bad party", "%", p0)
      } yield {
        val logEntry1 = result1.successfulLogEntry
        val logEntry2 = result2.successfulLogEntry
        logEntry1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY
        logEntry1.getPartyAllocationRejectionEntry.getReasonCase shouldEqual DamlPartyAllocationRejectionEntry.ReasonCase.INVALID_NAME
        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY
        logEntry2.getPartyAllocationRejectionEntry.getReasonCase shouldEqual DamlPartyAllocationRejectionEntry.ReasonCase.INVALID_NAME

      }
    }

    "reject on duplicate party" in KVTest.runTest {
      for {
        result1 <- preExecutePartyAllocation("alice", "alice", p0)
        result2 <- preExecutePartyAllocation("alice again", "alice", p0)
      } yield {
        val logEntry1 = result1.successfulLogEntry
        val logEntry2 = result2.successfulLogEntry
        logEntry1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY
        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY
        logEntry2.getPartyAllocationRejectionEntry.getReasonCase shouldEqual DamlPartyAllocationRejectionEntry.ReasonCase.ALREADY_EXISTS
      }
    }

    "reject duplicate submission" in KVTest.runTest {
      for {
        result0 <- preExecutePartyAllocation("submission-1", "alice", p0)
        result1 <- preExecutePartyAllocation("submission-1", "bob", p0)
      } yield {
        val logEntry0 = result0.successfulLogEntry
        val logEntry1 = result1.successfulLogEntry
        logEntry0.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY
        logEntry1.getPayloadCase shouldEqual
          DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY
        logEntry1.getPartyAllocationRejectionEntry.getReasonCase shouldEqual
          DamlPartyAllocationRejectionEntry.ReasonCase.DUPLICATE_SUBMISSION
      }
    }

    "update metrics" in KVTest.runTest {
      for {
        //Submit party twice to force one acceptance and one rejection on duplicate
        _ <- preExecutePartyAllocation("submission-1", "alice", p0)
        _ <- preExecutePartyAllocation("submission-1", "bob", p0)
      } yield {
        // Check that we're updating the metrics (assuming this test at least has been run)
        metrics.daml.kvutils.committer.partyAllocation.accepts.getCount should be >= 1L
        metrics.daml.kvutils.committer.partyAllocation.rejections.getCount should be >= 1L
        metrics.daml.kvutils.committer.runTimer("party_allocation").getCount should be >= 1L
      }
    }
  }
}
