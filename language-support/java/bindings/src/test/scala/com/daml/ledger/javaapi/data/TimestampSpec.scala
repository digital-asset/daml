// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import java.time.Instant

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class TimestampSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  behavior of "Timestamp"

  it should "be built from a java.time.Instant" in forAll(
    Gen.oneOf(0L, 1L, 10L, 100L, 1000L, Instant.now().toEpochMilli)) { millis =>
    val instant = java.time.Instant.ofEpochMilli(millis)
    withClue(
      s"input: ${millis}ms instant.getEpochSeconds: ${instant.getEpochSecond} instant.getNanos: ${instant.getNano} issue: ") {
      Timestamp
        .fromInstant(instant)
        .getMicroseconds shouldBe (millis * 1000) // getValue gives back microseconds
    }
  }

  it should "lose nanoseconds when doing TimeStamp.fromInstant(_).toInstant()" in {
    val instant = java.time.Instant.ofEpochSecond(1, 42)
    val timestamp = Timestamp.fromInstant(instant)
    timestamp.toInstant shouldBe Instant.ofEpochSecond(1, 0)
  }
}
