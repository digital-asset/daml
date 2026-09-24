// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.time.fetcher

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.client.time.fetcher.SequencingTimeFetcherTest.{
  sequencerIds,
  ts,
}
import com.digitalasset.canton.sequencing.client.time.fetcher.SequencingTimeReadings.TimeReading
import com.digitalasset.canton.time.{
  Clock,
  NonNegativeFiniteDuration,
  PositiveFiniteDuration,
  SimClock,
}
import com.digitalasset.canton.topology.SequencerId
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference

class ExpiringInMemorySequencingTimeReadingsTest extends AnyWordSpec with BaseTest {

  "Getting time readings" should {
    "expire old time readings and filter time readings as requested" in {
      val t = CantonTimestamp.MaxValue
      val timesRef =
        new AtomicReference(
          Map(
            sequencerIds(0) -> TimeReading(
              reading = Some(t),
              receivedAt = CantonTimestamp.Epoch,
            ),
            sequencerIds(1) -> TimeReading(
              reading = Some(t),
              receivedAt = ts(1),
            ),
            sequencerIds(2) -> TimeReading(
              reading = Some(t),
              receivedAt = ts(2),
            ),
          )
        )
      val readings =
        newTimeReadings(localClock = new SimClock(ts(2), loggerFactory), timesRef = timesRef)

      val expectedUnexpiredTimes =
        Map(
          sequencerIds(1) -> TimeReading(
            reading = Some(t),
            receivedAt = ts(1),
          ),
          sequencerIds(2) -> TimeReading(
            reading = Some(t),
            receivedAt = ts(2),
          ),
        )

      readings.getTimeReadings(maxTimeReadingsAge = None) shouldBe expectedUnexpiredTimes
      timesRef.get() shouldBe expectedUnexpiredTimes

      readings.getTimeReadings(maxTimeReadingsAge =
        Some(NonNegativeFiniteDuration.tryOfSeconds(1))
      ) shouldBe
        Map(
          sequencerIds(2) -> TimeReading(
            reading = Some(t),
            receivedAt = ts(2),
          )
        )
      timesRef.get() shouldBe expectedUnexpiredTimes

      readings
        .getTimeReadings(maxTimeReadingsAge =
          Some(NonNegativeFiniteDuration.tryOfSeconds(0))
        ) shouldBe Map.empty
    }
  }

  "Computing the valid sequencing time interval" should {
    "work correctly" in {
      val readings = newTimeReadings()
      Table(
        ("number of timestamps", "trust threshold", "expected result"),
        (0, PositiveInt.tryCreate(1), None),
        (1, PositiveInt.tryCreate(1), Some(ts(0) -> ts(0))),
        (2, PositiveInt.tryCreate(1), Some(ts(0) -> ts(1))),
        (2, PositiveInt.tryCreate(2), None),
        (3, PositiveInt.tryCreate(2), Some(ts(1) -> ts(1))),
        (4, PositiveInt.tryCreate(2), Some(ts(1) -> ts(2))),
        (4, PositiveInt.tryCreate(3), None),
        (5, PositiveInt.tryCreate(3), Some(ts(2) -> ts(2))),
        (6, PositiveInt.tryCreate(3), Some(ts(2) -> ts(3))),
      ).forEvery { case (numTimestamps, trustThreshold, expectedResult) =>
        val timestamps = (0 until numTimestamps).map(i => ts(i)).toVector
        readings.validTimeInterval(
          timestamps,
          trustThreshold,
        ) shouldBe expectedResult
      }
    }
  }

  "Recording a reading" should {
    "keep the latest reading and its reception time" in {
      val timesRef =
        new AtomicReference(
          Map(
            sequencerIds(0) -> TimeReading(Some(ts(0)), ts(0)),
            sequencerIds(1) -> TimeReading(Some(ts(1)), ts(0)),
            sequencerIds(2) -> TimeReading(Some(ts(1)), ts(0)),
          )
        )
      val readings = newTimeReadings(timesRef = timesRef)

      readings.recordReading(sequencerIds(0), Some(ts(0)), ts(1))
      readings.recordReading(sequencerIds(1), Some(ts(0)), ts(1))
      readings.recordReading(sequencerIds(2), Some(ts(2)), ts(2))
      readings.recordReading(sequencerIds(3), Some(ts(3)), ts(3))

      timesRef.get() shouldBe Map(
        sequencerIds(0) -> TimeReading(Some(ts(0)), ts(1)),
        sequencerIds(1) -> TimeReading(Some(ts(1)), ts(0)),
        sequencerIds(2) -> TimeReading(Some(ts(2)), ts(2)),
        sequencerIds(3) -> TimeReading(Some(ts(3)), ts(3)),
      )
    }
  }

  private def newTimeReadings(
      localClock: Clock = wallClock,
      timesRef: AtomicReference[Map[SequencerId, TimeReading]] = new AtomicReference(),
  ) =
    new ExpiringInMemorySequencingTimeReadings(
      localClock,
      timeReadingsRetention = PositiveFiniteDuration.tryOfSeconds(2),
      loggerFactory = loggerFactory,
      timesRef,
    )
}
