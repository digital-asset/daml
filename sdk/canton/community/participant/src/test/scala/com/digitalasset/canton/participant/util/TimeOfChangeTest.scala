// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{BaseTest, RepairCounter, RequestCounter}
import org.scalatest.wordspec.AnyWordSpec

class TimeOfChangeTest extends AnyWordSpec with BaseTest {
  "TimeOfChange" should {
    "order non-repairs before repairs" in {
      TimeOfChange(CantonTimestamp.ofEpochSecond(1)) should be < TimeOfChange(
        CantonTimestamp
          .ofEpochSecond(1),
        Some(RepairCounter.Genesis),
      )
    }

    "order by timestamp" in {
      TimeOfChange(CantonTimestamp.ofEpochSecond(1)) should be < TimeOfChange(
        CantonTimestamp.ofEpochSecond(2)
      )
    }

    "order by repair counter under the same timestamp" in {
      TimeOfChange(
        CantonTimestamp.ofEpochSecond(1),
        Some(RepairCounter.Genesis),
      ) should be < TimeOfChange(
        CantonTimestamp.ofEpochSecond(1),
        Some(RepairCounter.One),
      )
    }

    "order giving precedence to timestamp over repair counter" in {
      TimeOfChange(
        CantonTimestamp.ofEpochSecond(1),
        Some(RepairCounter.Genesis),
      ) should be < TimeOfChange(
        CantonTimestamp.ofEpochSecond(2),
        Some(RepairCounter.One),
      )

      TimeOfChange(
        CantonTimestamp.ofEpochSecond(1),
        Some(RepairCounter.Genesis),
      ) should be < TimeOfChange(CantonTimestamp.ofEpochSecond(2))
    }
  }

  "TimeOfRequest" should {
    "order by request counters" in {
      TimeOfRequest(
        RequestCounter.Genesis,
        CantonTimestamp.ofEpochSecond(1),
      ) should be < TimeOfRequest(
        RequestCounter.One,
        CantonTimestamp.ofEpochSecond(1),
      )
    }

    "order by timestamp" in {
      TimeOfRequest(
        RequestCounter.Genesis,
        CantonTimestamp.ofEpochSecond(1),
      ) should be < TimeOfRequest(
        RequestCounter.Genesis,
        CantonTimestamp.ofEpochSecond(2),
      )
    }

    "order giving precedence to timestamp over request counter" in {
      TimeOfRequest(
        RequestCounter.One,
        CantonTimestamp.ofEpochSecond(1),
      ) should be < TimeOfRequest(
        RequestCounter.Genesis,
        CantonTimestamp.ofEpochSecond(2),
      )
    }
  }
}
