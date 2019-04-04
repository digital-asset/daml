// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.model

import java.time.{Instant, LocalDate}

import org.scalatest.{Matchers, WordSpec}

class ApiValueSpec extends WordSpec with Matchers {

  "Api values" when {
    "converting Date from ISO and back" should {
      val isoDate = "2019-01-28"
      val result = ApiDate.fromIso8601(isoDate).toIso8601

      "not change the value" in {
        result shouldBe isoDate
      }
    }

    "converting Date to ISO and back" should {
      val date = ApiDate(10000)
      val result = ApiDate.fromIso8601(date.toIso8601)

      "not change the value" in {
        result shouldBe date
      }
    }

    "converting Date from LocalDate and back" should {
      val localDate = LocalDate.of(2019, 1, 28)
      val result = ApiDate.fromLocalDate(localDate).toLocalDate

      "not change the value" in {
        result shouldBe localDate
      }
    }

    "converting Timestamp from ISO and back" should {
      // Timestamp has microsecond resolution
      val isoDateTime = "2019-01-28T12:44:33.123456Z"
      val result = ApiTimestamp.fromIso8601(isoDateTime).toIso8601

      "not change the value" in {
        result shouldBe isoDateTime
      }
    }

    "converting Timestamp to ISO and back" should {
      val timestamp = ApiTimestamp(123456789123456L)
      val result = ApiTimestamp.fromIso8601(timestamp.toIso8601)

      "not change the value" in {
        result shouldBe timestamp
      }
    }

    "converting Timestamp from Instant and back" should {
      // Timestamp has microsecond resolution
      val instant = Instant.ofEpochSecond(86400L * 365L * 30L, 123456L * 1000L)
      val result = ApiTimestamp.fromInstant(instant).toInstant

      "not change the value" in {
        result shouldBe instant
      }
    }

  }
}
