// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class CantonTimestampSecondTest extends AnyWordSpec with BaseTest {

  "CantonTimestampSecond" should {

    "have factory method to build from Instant" in {
      val i = Instant.parse("2022-12-12T12:00:00Z")
      CantonTimestampSecond.fromInstant(i).value.underlying.toInstant shouldBe i

      CantonTimestampSecond
        .fromInstant(Instant.parse("2022-12-12T12:00:00.500Z"))
        .left
        .value shouldBe a[String]
    }

    "have factory method to build from CantonTimestamp" in {
      val tsRounded = CantonTimestamp.fromInstant(Instant.parse("2022-12-12T12:00:00Z")).value
      val tsNotRounded = tsRounded + NonNegativeFiniteDuration.tryOfMillis(500)

      CantonTimestampSecond
        .fromCantonTimestamp(tsRounded)
        .value
        .underlying
        .toInstant shouldBe tsRounded.toInstant

      CantonTimestampSecond
        .fromCantonTimestamp(tsNotRounded)
        .left
        .value shouldBe a[String]
    }

    "have floor factory method" in {
      val instant0 = Instant.parse("1500-06-12T12:00:00Z")
      val instant1 = Instant.parse("1500-06-12T12:00:00.200Z")
      val instant2 = Instant.parse("1500-06-12T12:00:00.500Z")
      val instant3 = Instant.parse("1500-06-12T12:00:00Z")

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant0).value)
        .toInstant shouldBe instant0

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant1).value)
        .toInstant shouldBe instant0

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant2).value)
        .toInstant shouldBe instant0

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant3).value)
        .toInstant shouldBe instant3
    }

    "have ceil factory method" in {
      val instant0 = Instant.parse("1500-06-12T12:00:00Z")
      val instant1 = Instant.parse("1500-06-12T12:00:00.200Z")
      val instant2 = Instant.parse("1500-06-12T12:00:00.500Z")
      val instant3 = Instant.parse("1500-06-12T12:00:00Z")

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant0).value)
        .toInstant shouldBe instant0

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant1).value)
        .toInstant shouldBe instant3

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant2).value)
        .toInstant shouldBe instant3

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant3).value)
        .toInstant shouldBe instant3
    }
  }
}
