// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  DynamicSynchronizerParametersWithValidity,
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

import SequencerUtils.maxSequencingTimeUpperBoundAt

class SequencerUtilsTest extends AnyWordSpec with BaseTest with Matchers {

  private val defaults: DynamicSynchronizerParameters =
    DynamicSynchronizerParameters.defaultValues(testedProtocolVersion)

  private implicit def secondsToNonNegativeFiniteDuration(seconds: Int): NonNegativeFiniteDuration =
    NonNegativeFiniteDuration(PositiveSeconds.tryOfSeconds(seconds.toLong))

  private def ts(seconds: Long): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(seconds)

  private def makeParametersWithValidity(
      validFrom: CantonTimestamp,
      validUntil: Option[CantonTimestamp],
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration =
        defaults.sequencerAggregateSubmissionTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = defaults.mediatorReactionTimeout,
      confirmationResponseTimeout: NonNegativeFiniteDuration = defaults.confirmationResponseTimeout,
  ): DynamicSynchronizerParametersWithValidity = {
    val parameters = DynamicSynchronizerParameters.tryInitialValues(
      topologyChangeDelay = NonNegativeFiniteDuration.Zero,
      protocolVersion = testedProtocolVersion,
      sequencerAggregateSubmissionTimeout = sequencerAggregateSubmissionTimeout,
      mediatorReactionTimeout = mediatorReactionTimeout,
      confirmationResponseTimeout = confirmationResponseTimeout,
    )

    DynamicSynchronizerParametersWithValidity(
      parameters = parameters, // Placeholder for actual parameters
      validFrom = validFrom,
      validUntil = validUntil,
    )
  }

  "maxSequencingTimeUpperBoundAt" should {
    "produce correct results" in {
      val parameterChanges1 = Seq(
        makeParametersWithValidity(
          sequencerAggregateSubmissionTimeout = 100,
          validFrom = CantonTimestamp.Epoch,
          validUntil = None,
        )
      )

      maxSequencingTimeUpperBoundAt(ts(0), parameterChanges1) shouldEqual ts(100)
      maxSequencingTimeUpperBoundAt(ts(50), parameterChanges1) shouldEqual ts(150)
      maxSequencingTimeUpperBoundAt(ts(100), parameterChanges1) shouldEqual ts(200)
      maxSequencingTimeUpperBoundAt(ts(150), parameterChanges1) shouldEqual ts(250)

      val parameterChanges2 = Seq(
        makeParametersWithValidity(
          sequencerAggregateSubmissionTimeout = 1000,
          validFrom = CantonTimestamp.MinValue,
          validUntil = Some(ts(200)),
        ),
        makeParametersWithValidity(
          sequencerAggregateSubmissionTimeout = 100,
          validFrom = ts(200),
          validUntil = None,
        ),
      )

      maxSequencingTimeUpperBoundAt(ts(0), parameterChanges2) shouldEqual ts(1000)
      maxSequencingTimeUpperBoundAt(ts(200), parameterChanges2) shouldEqual ts(1200)
      maxSequencingTimeUpperBoundAt(ts(250), parameterChanges2) shouldEqual ts(1200)
      maxSequencingTimeUpperBoundAt(ts(1100), parameterChanges2) shouldEqual ts(1200)
      maxSequencingTimeUpperBoundAt(ts(1101), parameterChanges2) shouldEqual ts(1201)
      maxSequencingTimeUpperBoundAt(ts(1200), parameterChanges2) shouldEqual ts(1300)
    }
  }

  "timeOffsetPastSynchronizerUpgrade" should {
    "produce correct results" in {
      // Ordinary case, just the confirmationResponseTimeout + mediatorReactionTimeout
      val parameterChanges1 = Seq(
        makeParametersWithValidity(
          confirmationResponseTimeout = 100,
          mediatorReactionTimeout = 100,
          validFrom = CantonTimestamp.Epoch,
          validUntil = None,
        )
      )
      SequencerUtils
        .timeOffsetPastSynchronizerUpgrade(
          upgradeTime = ts(1000),
          parameterChanges = parameterChanges1,
        )
        .duration
        .toSeconds shouldBe 200L

      // Extraordinary case, max possible decision time from earlier parameters = ts + timeouts = 1000 + 2000 = 3000
      val parameterChanges2 = Seq(
        makeParametersWithValidity(
          confirmationResponseTimeout = 1000,
          mediatorReactionTimeout = 1000,
          validFrom = CantonTimestamp.Epoch,
          validUntil = Some(ts(1000)),
        ),
        makeParametersWithValidity(
          confirmationResponseTimeout = 100,
          mediatorReactionTimeout = 100,
          validFrom = ts(1000),
          validUntil = None,
        ),
      )

      // At upgrade time 1200 we need to shift by 1800 to reach 3000 (see above)
      SequencerUtils
        .timeOffsetPastSynchronizerUpgrade(
          upgradeTime = ts(1200),
          parameterChanges = parameterChanges2,
        )
        .duration
        .toSeconds shouldBe 1800L

      // At upgrade time 1400 we need to shift by 1600 to reach 3000 (see above)
      SequencerUtils
        .timeOffsetPastSynchronizerUpgrade(
          upgradeTime = ts(1400),
          parameterChanges = parameterChanges2,
        )
        .duration
        .toSeconds shouldBe 1600L

      // At upgrade time 4000 and above we need to shift by the ordinary 200
      SequencerUtils
        .timeOffsetPastSynchronizerUpgrade(
          upgradeTime = ts(4000),
          parameterChanges = parameterChanges2,
        )
        .duration
        .toSeconds shouldBe 200L
    }
  }
}
