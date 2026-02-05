// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.integrations.state

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.synchronizer.sequencing.integrations.state.DbSequencerStateManagerStore.getInFlightAggregationIntervals
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

class InFlightAggregationsIntervalTest extends AsyncWordSpec with BaseTest {

  def suppressLogWarnings(testCode: => Assertion): Assertion =
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      testCode,
      LogEntry.assertLogSeq(
        Seq(
          (
            _.warningMessage should include(
              "Disabling aggregator time interval batching as an excessive number of aggregator batches would otherwise be generated"
            ),
            "warning that in-flight aggregation queries are not being batched",
          )
        )
      ),
    )

  "sequencingTimeLowerBound > sequencingTimeUpperBound" in {
    val sequencingTimeUpperBound = CantonTimestamp.ofEpochMilli(1)
    val sequencingTimeLowerBound = CantonTimestamp.ofEpochMilli(2)

    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(1),
      sequencingTimeLowerBound,
      sequencingTimeUpperBound,
    ) shouldBe Seq.empty
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(2),
      sequencingTimeLowerBound,
      sequencingTimeUpperBound,
    ) shouldBe Seq.empty
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(2),
      CantonTimestamp.Epoch,
      CantonTimestamp.MinValue,
    ) shouldBe Seq.empty
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(2),
      CantonTimestamp.MaxValue,
      sequencingTimeUpperBound,
    ) shouldBe Seq.empty
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(2),
      CantonTimestamp.MaxValue,
      CantonTimestamp.Epoch,
    ) shouldBe Seq.empty
  }

  "sequencingTimeLowerBound == sequencingTimeUpperBound" in {
    val sequencingTimeUpperBound = CantonTimestamp.ofEpochMilli(1)
    val sequencingTimeLowerBound = CantonTimestamp.ofEpochMilli(1)

    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(1),
      sequencingTimeLowerBound,
      sequencingTimeUpperBound,
    ) shouldBe Seq.empty
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(2),
      sequencingTimeLowerBound,
      sequencingTimeUpperBound,
    ) shouldBe Seq.empty
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(2),
      CantonTimestamp.Epoch,
      CantonTimestamp.Epoch,
    ) shouldBe Seq.empty
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(2),
      CantonTimestamp.MinValue,
      CantonTimestamp.MinValue,
    ) shouldBe Seq.empty
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(2),
      CantonTimestamp.MaxValue,
      CantonTimestamp.MaxValue,
    ) shouldBe Seq.empty
  }

  "sequencingTimeLowerBound < sequencingTimeUpperBound" in {
    val batchingLimit = 10000L
    val sequencingTimeLargeUpperBound = CantonTimestamp.ofEpochMilli(batchingLimit + 1)
    val sequencingTimeLargeUpperBoundM1 = CantonTimestamp.ofEpochMilli(batchingLimit)
    val sequencingTimeBound5 = CantonTimestamp.ofEpochMilli(5)
    val sequencingTimeBound4 = CantonTimestamp.ofEpochMilli(4)
    val sequencingTimeBound3 = CantonTimestamp.ofEpochMilli(3)
    val sequencingTimeBound2 = CantonTimestamp.ofEpochMilli(2)
    val sequencingTimeBound1 = CantonTimestamp.ofEpochMilli(1)

    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(1),
      sequencingTimeBound1,
      sequencingTimeBound5,
    ) shouldBe Seq(
      sequencingTimeBound1 -> sequencingTimeBound2,
      sequencingTimeBound2 -> sequencingTimeBound3,
      sequencingTimeBound3 -> sequencingTimeBound4,
      sequencingTimeBound4 -> sequencingTimeBound5,
    )
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(2),
      sequencingTimeBound1,
      sequencingTimeBound5,
    ) shouldBe Seq(
      sequencingTimeBound1 -> sequencingTimeBound3,
      sequencingTimeBound3 -> sequencingTimeBound5,
    )
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(3),
      sequencingTimeBound1,
      sequencingTimeBound5,
    ) shouldBe Seq(
      sequencingTimeBound1 -> sequencingTimeBound4,
      sequencingTimeBound4 -> sequencingTimeBound5,
    )
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(4),
      sequencingTimeBound1,
      sequencingTimeBound5,
    ) shouldBe Seq(
      sequencingTimeBound1 -> sequencingTimeBound5
    )
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(5),
      sequencingTimeBound1,
      sequencingTimeBound5,
    ) shouldBe Seq(sequencingTimeBound1 -> sequencingTimeBound5)
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(6),
      sequencingTimeBound1,
      sequencingTimeBound5,
    ) shouldBe Seq(sequencingTimeBound1 -> sequencingTimeBound5)
    suppressLogWarnings {
      getInFlightAggregationIntervals(
        PositiveFiniteDuration.ofMillis(4),
        CantonTimestamp.MinValue,
        CantonTimestamp.MaxValue,
      ) shouldBe Seq(CantonTimestamp.MinValue -> CantonTimestamp.MaxValue)
    }
    suppressLogWarnings {
      getInFlightAggregationIntervals(
        PositiveFiniteDuration.ofMillis(1),
        sequencingTimeBound1,
        sequencingTimeLargeUpperBound,
      ) shouldBe Seq(sequencingTimeBound1 -> sequencingTimeLargeUpperBound)
    }
    getInFlightAggregationIntervals(
      PositiveFiniteDuration.ofMillis(1),
      sequencingTimeBound1,
      sequencingTimeLargeUpperBoundM1,
    ).length shouldBe batchingLimit - 1
  }
}
