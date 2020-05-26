// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class SimpleTimeServiceBackendSpec extends WordSpec with Matchers with ScalaFutures {
  "a simple time service backend" should {
    "return the time it started with" in {
      val timeService = TimeServiceBackend.simple(instantAt(month = 1))
      timeService.getCurrentTime should be(instantAt(month = 1))
    }

    "update the time to a new time" in {
      val timeService = TimeServiceBackend.simple(instantAt(month = 1))
      timeService.setCurrentTime(instantAt(month = 1), instantAt(month = 2))
      timeService.getCurrentTime should be(instantAt(month = 2))
    }

    "not allow the time to be updated without a correct expected time" in {
      val timeService = TimeServiceBackend.simple(instantAt(month = 1))
      whenReady(timeService.setCurrentTime(instantAt(month = 1), instantAt(month = 2))) {
        _ should be(true)
      }
      whenReady(timeService.setCurrentTime(instantAt(month = 1), instantAt(month = 3))) {
        _ should be(false)
      }
      timeService.getCurrentTime should be(instantAt(month = 2))
    }
  }

  // always construct new instants to avoid sharing references, which would allow us to cheat when
  // comparing them inside the SimpleTimeServiceBackend
  private def instantAt(month: Int): Instant =
    ZonedDateTime.of(2020, month, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
}
