// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.SimClock

import java.time

class PartitionedTimestampGeneratorTest extends BaseTestWordSpec {
  "ensures timestamps are unique for many writers even if time doesn't move" in {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val generator1 = new PartitionedTimestampGenerator(clock, 0, PositiveInt.tryCreate(3))
    val generator2 = new PartitionedTimestampGenerator(clock, 1, PositiveInt.tryCreate(3))
    val generator3 = new PartitionedTimestampGenerator(clock, 2, PositiveInt.tryCreate(3))

    val g1ts1 = generator1.generateNext
    val g2ts1 = generator2.generateNext
    val g3ts1 = generator3.generateNext
    val g1ts2 = generator1.generateNext
    val g2ts2 = generator2.generateNext
    val g3ts2 = generator3.generateNext
    val firstRound = NonEmpty(Set, g1ts1, g2ts1, g3ts1)
    val secondRound = NonEmpty(Set, g1ts2, g2ts2, g3ts2)
    val allTimestamps = firstRound ++ secondRound

    withClue("should all be unique") {
      allTimestamps.forgetNE should have size (6)
    }

    withClue("first round timestamps should be before second round") {
      forAll(firstRound.forgetNE) { ts =>
        ts shouldBe <(secondRound.head1)
      }
    }
  }

  "generating faster than clock is advancing" in {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val generator1 = new PartitionedTimestampGenerator(clock, 0, PositiveInt.tryCreate(2))
    val generator2 = new PartitionedTimestampGenerator(clock, 1, PositiveInt.tryCreate(2))

    val timestamps = (0 until 10) flatMap { _ =>
      val ts1 = generator1.generateNext
      val ts2 = generator2.generateNext

      clock.advance(time.Duration.ofNanos(1000))

      Seq(ts1, ts2)
    }

    withClue("timestamps should be unique") {
      timestamps.toSet should have size (timestamps.size.toLong)
    }

    withClue("should increase") {
      NonEmptyUtil.fromUnsafe(timestamps).reduceLeft[CantonTimestamp] { case (first, second) =>
        first shouldBe <(second)
        second
      }
    }
  }
}
