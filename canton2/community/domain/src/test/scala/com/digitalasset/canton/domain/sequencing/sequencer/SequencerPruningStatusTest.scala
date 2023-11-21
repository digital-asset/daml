// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.option.*
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.DefaultTestIdentities

class SequencerPruningStatusTest extends BaseTestWordSpec {
  def ts(epochSeconds: Int): CantonTimestamp =
    CantonTimestamp.Epoch.plusSeconds(epochSeconds.toLong)

  "safe pruning point" should {
    "use the earliest acknowledgment timestamp if all members have recorded read clients" in {
      val status = SequencerPruningStatus(
        lowerBound = ts(0),
        now = ts(15),
        members = Seq(
          SequencerMemberStatus(
            member = DefaultTestIdentities.sequencerId,
            registeredAt = ts(8),
            ts(10).some,
          ),
          SequencerMemberStatus(
            member = DefaultTestIdentities.mediator,
            registeredAt = ts(7),
            ts(12).some,
          ),
        ),
      )

      // should pick the earliest ack timestamp
      status.safePruningTimestamp shouldBe ts(10)
    }

    "use the registered at time for a member if no data has yet been read" in {
      val status = SequencerPruningStatus(
        lowerBound = ts(0),
        now = ts(15),
        members = Seq(
          SequencerMemberStatus(
            member = DefaultTestIdentities.sequencerId,
            registeredAt = ts(8),
            ts(15).some,
          ),
          SequencerMemberStatus(
            member = DefaultTestIdentities.mediator,
            registeredAt = ts(7),
            None,
          ),
        ),
      )

      // should pick the mediator's registeredAt timestamp
      status.safePruningTimestamp shouldBe ts(7)
    }

    "return the lowest registered at if data has not been read" in {
      val status = SequencerPruningStatus(
        lowerBound = ts(0),
        now = ts(12),
        members = Seq(
          SequencerMemberStatus(
            member = DefaultTestIdentities.sequencerId,
            registeredAt = ts(3),
            None,
          )
        ),
      )

      status.safePruningTimestamp shouldBe ts(3)
    }

    "ignore member entirely if they are disabled" in {
      val status = SequencerPruningStatus(
        lowerBound = ts(0),
        now = ts(12),
        members = Seq(
          SequencerMemberStatus(
            member = DefaultTestIdentities.sequencerId,
            registeredAt = ts(1),
            ts(2).some,
            enabled = false, // this is the key bit..
          ),
          SequencerMemberStatus(
            member = DefaultTestIdentities.mediator,
            registeredAt = ts(2),
            ts(12).some,
          ),
        ),
      )

      // as although the sequencer hasn't read up until this point, it is disabled
      status.safePruningTimestamp shouldBe ts(12)
    }
  }
}
