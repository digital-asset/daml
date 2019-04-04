// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.time.Instant

import com.google.protobuf.Timestamp
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import Generators._

class TimestampComparatorTest extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  behavior of "TimestampComparator.compare"

  it should "return 0 for the default value" in {
    TimestampComparator.compare(Timestamp.getDefaultInstance, Timestamp.getDefaultInstance) shouldBe 0
  }

  it should "return 0 for the same value" in forAll(timestampGen) { timestamp: Timestamp =>
    TimestampComparator.compare(timestamp, timestamp) shouldBe 0
  }

  it should "return <0 when the first value is smaller than the second one" in forAll(intervalGen) {
    case (smallerTimestamp: Timestamp, biggerTimestamp: Timestamp) =>
      TimestampComparator.compare(smallerTimestamp, biggerTimestamp) should be < 0
  }

  it should "return >0 when the first value is bigger than the second one" in forAll(intervalGen) {
    case (smallerTimestamp: Timestamp, biggerTimestamp: Timestamp) =>
      TimestampComparator.compare(biggerTimestamp, smallerTimestamp) should be > 0
  }
}

// TODO(mp): find better name and better home
object Generators {

  // https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/timestamp.proto#L125
  val minTimestampSeconds: Long = Instant.parse("0001-01-01T00:00:00Z").getEpochSecond
  val maxTimestampSeconds: Long = Instant.parse("9999-12-31T23:59:59Z").getEpochSecond
  val minTimestampNanos: Int = 0
  val maxTimestampNanos: Int = 999999999

  val minTimestamp: Timestamp =
    Timestamp.newBuilder().setSeconds(minTimestampSeconds).setNanos(minTimestampNanos).build()
  val maxTimestamp: Timestamp =
    Timestamp.newBuilder().setSeconds(maxTimestampSeconds).setNanos(maxTimestampNanos).build()

  val timestampGen: Gen[Timestamp] =
    for {
      seconds <- Gen.chooseNum(minTimestampSeconds, maxTimestampSeconds)
      nanos <- Gen.chooseNum(minTimestampNanos, maxTimestampNanos)
    } yield Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build()

  // generates an interval of timestamps where the left boundary is smaller than the right boundary
  val intervalGen: Gen[(Timestamp, Timestamp)] =
    for {
      intervalMin <- timestampGen.suchThat(!_.equals(maxTimestamp))
      intervalMax <- Gen
        .zip(
          Gen.chooseNum(intervalMin.getSeconds, maxTimestampSeconds),
          Gen.chooseNum(intervalMin.getNanos, maxTimestampNanos))
        .suchThat { case (s, n) => s != intervalMin.getSeconds || n != intervalMin.getNanos }
        .map { case (s, n) => Timestamp.newBuilder().setSeconds(s).setNanos(n).build() }
    } yield (intervalMin, intervalMax)
}
