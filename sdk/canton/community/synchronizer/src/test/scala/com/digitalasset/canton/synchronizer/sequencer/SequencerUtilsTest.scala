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

import SequencerUtils.maxSequencingTimeUpperBoundAt

class SequencerUtilsTest extends AnyWordSpec with BaseTest with Matchers {

  def parametersWithTimeoutAndValidity(
      timeoutSeconds: Long,
      validFrom: CantonTimestamp,
      validUntil: Option[CantonTimestamp],
  ): DynamicSynchronizerParametersWithValidity = {
    val parameters = DynamicSynchronizerParameters.tryInitialValues(
      topologyChangeDelay = NonNegativeFiniteDuration.Zero,
      protocolVersion = testedProtocolVersion,
      sequencerAggregateSubmissionTimeout = NonNegativeFiniteDuration(
        PositiveSeconds.tryOfSeconds(timeoutSeconds)
      ),
    )

    DynamicSynchronizerParametersWithValidity(
      parameters = parameters, // Placeholder for actual parameters
      validFrom = validFrom,
      validUntil = validUntil,
    )
  }

  def ts(seconds: Long): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(seconds)

  "maxSequencingTimeUpperBoundAt" should {
    "produce correct results" in {
      val parameterChanges1 = Seq(
        parametersWithTimeoutAndValidity(
          timeoutSeconds = 100,
          validFrom = CantonTimestamp.Epoch,
          validUntil = None,
        )
      )

      maxSequencingTimeUpperBoundAt(ts(0), parameterChanges1) shouldEqual ts(100)
      maxSequencingTimeUpperBoundAt(ts(50), parameterChanges1) shouldEqual ts(150)
      maxSequencingTimeUpperBoundAt(ts(100), parameterChanges1) shouldEqual ts(200)
      maxSequencingTimeUpperBoundAt(ts(150), parameterChanges1) shouldEqual ts(250)

      val parameterChanges2 = Seq(
        parametersWithTimeoutAndValidity(
          timeoutSeconds = 1000,
          validFrom = CantonTimestamp.MinValue,
          validUntil = Some(ts(200)),
        ),
        parametersWithTimeoutAndValidity(
          timeoutSeconds = 100,
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
}
