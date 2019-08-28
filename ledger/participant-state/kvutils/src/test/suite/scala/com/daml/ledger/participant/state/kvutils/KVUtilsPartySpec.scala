// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlPartyAllocationRejectionEntry
}
import org.scalatest.{Matchers, WordSpec}

class KVUtilsPartySpec extends WordSpec with Matchers {
  // TESTS:
  // - party allocation rejected if participant id does not match
  // - party allocation rejected with bad party string
  // - party allocation succeeds

  import KVTest._
  import TestHelpers._

  "party allocation" should {
    val p0 = mkParticipantId(0)
    val p1 = mkParticipantId(1)

    "succeed" in KVTest.runTest {
      withParticipantId(p0) {
        submitPartyAllocation("ok", "alice", p0).map { logEntry =>
          logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY
        }
      }
    }

    "reject when participant id does not match" in KVTest.runTest {
      withParticipantId(p0) {
        submitPartyAllocation("mismatch", "alice", p1)
      }.map { logEntry =>
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY
      }
    }

    "reject on bad party string" in KVTest.runTest {
      for {
        logEntry1 <- submitPartyAllocation("empty party", "", p0)
        logEntry2 <- submitPartyAllocation("bad party", "%", p0)
      } yield {
        logEntry1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY
        logEntry1.getPartyAllocationRejectionEntry.getReasonCase shouldEqual DamlPartyAllocationRejectionEntry.ReasonCase.INVALID_NAME
        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY
        logEntry2.getPartyAllocationRejectionEntry.getReasonCase shouldEqual DamlPartyAllocationRejectionEntry.ReasonCase.INVALID_NAME

      }

    }
  }

}
