// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model

import java.time.{Instant, LocalDate}

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.daml.lf.data.{Time => LfTime}
import com.daml.lf.value.Value.{ValueDate, ValueTimestamp}
import com.daml.lf.value.json.ApiValueImplicits._

class ApiValueSpec extends AnyWordSpec with Matchers {

  "Api values" when {
    "converting Date from ISO and back" should {
      val isoDate = "2019-01-28"
      val result = ValueDate.fromIso8601(isoDate).toIso8601

      "not change the value" in {
        result shouldBe isoDate
      }
    }

    "converting Date to ISO and back" should {
      val date = ValueDate(LfTime.Date assertFromDaysSinceEpoch 10000)
      val result = ValueDate.fromIso8601(date.toIso8601)

      "not change the value" in {
        result shouldBe date
      }
    }

    "converting Date from LocalDate and back" should {
      val localDate = LocalDate.of(2019, 1, 28)
      val result = ValueDate.fromLocalDate(localDate).toLocalDate

      "not change the value" in {
        result shouldBe localDate
      }
    }

    "converting Timestamp from ISO and back" should {
      // Timestamp has microsecond resolution
      val isoDateTime = "2019-01-28T12:44:33.123456Z"
      val result = ValueTimestamp.fromIso8601(isoDateTime).toIso8601

      "not change the value" in {
        result shouldBe isoDateTime
      }
    }

    "converting Timestamp to ISO and back" should {
      val timestamp = ValueTimestamp(LfTime.Timestamp assertFromLong 123456789123456L)
      val result = ValueTimestamp.fromIso8601(timestamp.toIso8601)

      "not change the value" in {
        result shouldBe timestamp
      }
    }

    "converting Timestamp from Instant and back" should {
      // Timestamp has microsecond resolution
      val instant = Instant.ofEpochSecond(86400L * 365L * 30L, 123456L * 1000L)
      val result = ValueTimestamp.fromInstant(instant).toInstant

      "not change the value" in {
        result shouldBe instant
      }
    }

  }
}
