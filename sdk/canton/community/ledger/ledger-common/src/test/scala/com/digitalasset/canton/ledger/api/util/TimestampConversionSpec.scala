// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.util

import com.daml.ledger.api.v2.value.Value.{Sum as VSum}
import com.digitalasset.daml.lf.data.Time
import org.scalacheck.{Gen, Prop}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.{Checkers, ScalaCheckDrivenPropertyChecks}

import java.time.Instant

import TimestampConversion.*
import Prop.exists

class TimestampConversionSpec
    extends AnyWordSpec
    with Matchers
    with Checkers
    with ScalaCheckDrivenPropertyChecks {
  import TimestampConversionSpec.*

  "instantToMicros" when {
    "given any instant without nanos" should {
      // documenting a known fact, more than "desired behavior"
      "overflow for some values" in {
        def prop(i: Instant): Prop = microsToInstant(instantToMicros(i)) != i
        check(prop(Instant parse "+959040998-10-20T14:33:31.896722Z") || exists(anyMicroTime)(prop))
      }
    }

    "given any instant with nanos in specified domain" should {
      "throw when unrepresentable with micros" in {
        def prop(i: Instant): Prop =
          try {
            instantToMicros(i)
            false
          } catch {
            case _: IllegalArgumentException => true
          }
        check(prop(Instant parse "7758-07-09T19:42:21.246906214Z") || exists(anyTimeInRange)(prop))
      }
    }

    "given any instant without nanos in specified domain" should {
      "be retracted by microsToInstant" in forAll(anyMicroInRange) { i =>
        microsToInstant(instantToMicros(i)) shouldBe i
      }

      "treat truncated instants likewise" in forAll(anyTimeInRange) { i =>
        val it = i truncatedTo java.time.temporal.ChronoUnit.MICROS
        microsToInstant(instantToMicros(it)) shouldBe it
      }
    }
  }

  "microsToInstant" when {
    "given any long value" should {
      "be total" in forAll { (ts: VSum.Timestamp) =>
        microsToInstant(ts) shouldBe microsToInstant(ts)
      }

      "be injective" in forAll { (ts1: VSum.Timestamp, ts2: VSum.Timestamp) =>
        whenever(ts1 != ts2) {
          microsToInstant(ts1) should not be microsToInstant(ts2)
        }
      }

      // documenting a known fact, more than "desired behavior"
      "overflow for some values" in {
        def prop(ts: VSum.Timestamp): Prop = instantToMicros(microsToInstant(ts)) != ts
        check(prop(VSum.Timestamp(-9223372036854775808L)) || exists(prop))
      }
    }

    "given a value in specified domain" should {
      "be retracted by instantToMicros" in forAll(timestampInRangeGen) { ts =>
        instantToMicros(microsToInstant(ts)) shouldBe ts
      }
    }
  }

  "fromLf" when {
    "given a value in specified domain" should {
      "be retracted by toLf" in forAll(lfTimestampGen) { ts =>
        toLf(fromLf(ts), ConversionMode.Exact) shouldBe ts
      }
    }
  }

  "toLf" when {
    "given a valid microsecond timestamp" should {
      "be retracted by fromLf" in forAll(anyMicroInRange) { ts =>
        val protoTs = fromInstant(ts)
        fromLf(toLf(protoTs, ConversionMode.Exact)) shouldBe protoTs
      }
    }

    "given a valid nanosecond timestamp" should {
      "round half up" in forAll(anyTimeInRange) { ts =>
        val protoTs = fromInstant(ts)
        val halfUp = toLf(protoTs, ConversionMode.HalfUp)
        halfUp.toInstant should be > ts.plusNanos(-500)
        halfUp.toInstant should be <= ts.plusNanos(500)
      }
    }
  }
}

object TimestampConversionSpec {
  import org.scalacheck.{Arbitrary, Shrink}
  import Arbitrary.arbitrary

  val timestampGen: Gen[VSum.Timestamp] = arbitrary[Long] map VSum.Timestamp
  implicit val timestampArb: Arbitrary[VSum.Timestamp] = Arbitrary(timestampGen)
  implicit val timestampShrink: Shrink[VSum.Timestamp] =
    Shrink(ts => Shrink.shrink(ts.value) map VSum.Timestamp)

  val timestampInRangeGen: Gen[VSum.Timestamp] =
    Gen.choose(instantToMicros(MIN).value, instantToMicros(MAX).value) map VSum.Timestamp

  def timeGen(min: Instant, max: Instant, microsOnly: Boolean): Gen[Instant] =
    Gen
      .zip(
        Gen.choose(min.getEpochSecond, max.getEpochSecond),
        if (microsOnly) Gen.choose(0L, 999999).map(_ * 1000)
        else Gen.choose(0L, 999999999),
      )
      .map { case (s, n) => Instant.ofEpochSecond(s, n) }

  val anyMicroTime: Gen[Instant] = timeGen(Instant.MIN, Instant.MAX, microsOnly = true)

  val anyTimeInRange: Gen[Instant] = timeGen(MIN, MAX, microsOnly = false)

  val anyMicroInRange: Gen[Instant] =
    timeGen(MIN, MAX, microsOnly = true)

  val lfTimestampGen: Gen[Time.Timestamp] = Gen.choose(
    Time.Timestamp.MinValue.micros,
    Time.Timestamp.MaxValue.micros,
  ) map Time.Timestamp.assertFromLong
}
