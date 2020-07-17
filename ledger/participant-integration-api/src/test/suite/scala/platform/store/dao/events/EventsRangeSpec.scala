// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao.events

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class EventsRangeSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  behavior of EventsRange.getClass.getSimpleName

  "isEmpty" should "work" in forAll(eventsRangeGen) { range =>
    val expected = range.startExclusive >= range.endInclusive
    EventsRange.isEmpty(range) shouldBe expected
  }

  private val eventsRangeGen: Gen[EventsRange[Int]] =
    for {
      a <- Arbitrary.arbitrary[Int]
      b <- Arbitrary.arbitrary[Int]
    } yield EventsRange(a, b)
}
