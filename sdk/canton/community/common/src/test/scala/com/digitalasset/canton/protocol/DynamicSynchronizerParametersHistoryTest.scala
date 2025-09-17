// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.DynamicSynchronizerParametersHistory.latestDecisionDeadline
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.wordspec.AnyWordSpec

class DynamicSynchronizerParametersHistoryTest extends AnyWordSpec with BaseTest {

  private val lowerBound = CantonTimestamp.ofEpochSecond(1000L)

  // Default "decision timeout" of 1 minute
  private val paramsDefault: DynamicSynchronizerParameters =
    DynamicSynchronizerParameters.defaultValues(testedProtocolVersion)

  // A bit longer than default "decision timeout" of 90s
  private val paramsLonger = paramsDefault.tryUpdate(
    confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(45L),
    mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfSeconds(45L),
  )

  // Huge decision timeout of 90 minutes, possibly due to an operator configuration mistake error scenario
  private val paramsHuge = paramsDefault.tryUpdate(
    confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfMinutes(45L),
    mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfMinutes(45L),
  )

  "latestDecisionDeadline" should {

    "return the lowerBound for an empty history" in {
      val history = Seq.empty[DynamicSynchronizerParametersWithValidity]

      latestDecisionDeadline(history, lowerBound) shouldBe lowerBound
    }

    "calculate the deadline for a history with only one, currently valid entry" in {
      val history = Seq(
        DynamicSynchronizerParametersWithValidity(paramsDefault, CantonTimestamp.Epoch, None)
      )

      // The deadline is calculated from the lowerBound, as it's the latest activeness time
      // for the currently valid parameters.
      val expectedDeadline = lowerBound
        .add(paramsDefault.confirmationResponseTimeout.unwrap)
        .add(paramsDefault.mediatorReactionTimeout.unwrap) // 1000s + 60s = 1060s

      latestDecisionDeadline(history, lowerBound) shouldBe expectedDeadline
    }

    "select the deadline from the currently active parameters if it is the latest" in {
      val ts200 = CantonTimestamp.ofEpochSecond(200L)
      val history = Seq(
        // A past change with a default timeout
        DynamicSynchronizerParametersWithValidity(
          paramsDefault,
          CantonTimestamp.Epoch,
          Some(ts200),
        ),
        // The currently active parameters with a longer timeout
        DynamicSynchronizerParametersWithValidity(paramsLonger, ts200, None),
      )

      // Deadlines:
      // 1. Past change: 200s (validUntil) + 60s = 260s
      // 2. Current change: 1000s (lowerBound) + 90s = 1090s
      // The maximum of (lowerBound, 260, 1090) is 1090.
      val expectedDeadline = CantonTimestamp.ofEpochSecond(1090L)

      latestDecisionDeadline(history, lowerBound) shouldBe expectedDeadline
    }

    "consider a past configuration (mistake) resulting in a huge decision timeout" in {
      // This scenario models: Default -> Huge decision timeout -> Default (currently active)
      val startOfDefaults = CantonTimestamp.ofEpochSecond(100L)
      val startOfHugeParams = CantonTimestamp.ofEpochSecond(200L)
      val endOfHugeParams = CantonTimestamp.ofEpochSecond(300L)

      val history = Seq(
        DynamicSynchronizerParametersWithValidity(
          paramsDefault,
          startOfDefaults,
          Some(startOfHugeParams),
        ),
        DynamicSynchronizerParametersWithValidity(
          paramsHuge,
          startOfHugeParams,
          Some(endOfHugeParams),
        ),
        DynamicSynchronizerParametersWithValidity(paramsDefault, endOfHugeParams, None),
      )

      // Deadlines:
      // 1. Default period 1: 200s (validUntil) + 60s = 260s
      // 2. Huge period: 300s (validUntil) + 5400s = 5700s
      // 3. Current default period: 1000s (lowerBound) + 60s = 1060s
      // The maximum of (lowerBound, 260, 5700, 1060) is 5700.
      val expectedDeadline = CantonTimestamp.ofEpochSecond(5700L)

      latestDecisionDeadline(history, lowerBound) shouldBe expectedDeadline
    }
  }
}
